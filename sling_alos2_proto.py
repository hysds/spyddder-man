#!/usr/bin/env python
"""
Sling data from a source to a destination:

  1) download data from a source and verify,
  2) push verified data to repository,
  3) submit extract-ingest job.

HTTP/HTTPS, FTP and OAuth authentication is handled using .netrc.
"""

import datetime, os, sys, re, requests, json, logging, traceback, argparse, shutil, glob
import tarfile, zipfile
from urlparse import urlparse
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3.exceptions import InsecurePlatformWarning

import boto

import osaka.main

import ConfigParser
import StringIO

from hysds.orchestrator import submit_job
import hysds.orchestrator
from hysds.celery import app
from hysds.dataset_ingest import ingest
from hysds_commons.job_rest_utils import single_process_and_submission

# disable warnings for SSL verification
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)

log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

# all file types
ALL_TYPES = []

# zip types
ZIP_TYPE = ["zip"]
ALL_TYPES.extend(ZIP_TYPE)

# tar types
# TAR_TYPE = ["tbz2", "tgz", "bz2", "gz"]
# ALL_TYPES.extend(TAR_TYPE)


def verify(path, file_type):
    """Verify downloaded file is okay by checking that it can
       be unzipped/untarred."""

    test_dir = "./extract_test"
    if file_type in ZIP_TYPE:
        if not zipfile.is_zipfile(path):
            raise RuntimeError("%s is not a zipfile." % path)
        with zipfile.ZipFile(path, 'r') as f:
            f.extractall(test_dir)
        shutil.rmtree(test_dir, ignore_errors=True)
    else:
        raise NotImplementedError("Failed to verify %s is file type %s." % \
                                  (path, file_type))


def extract(zip_file):
    """Extract the zipfile."""

    with zipfile.ZipFile(zip_file, 'r') as zf:
        zf.extractall()
    prod_dir = zip_file.replace(".zip", "")
    return prod_dir

# def harvest(mtd_config, extracted_dir, productType):
#     """Harvest the metadata for this product."""
#
#     metf = extracted_name+".met.json"
#     dsf = extracted_name+".dataset.json"
#     # mis_char = MISSION_RE.search(extracted).group(1)
#     # if productType == "slc" or productType == "raw":
#     fn = "%s/summary.txt" % extracted_dir
#     create_met_json.create_met_json(fn,metf,mis_char)
#     _create_dataset_json(extracted,metf,dsf)
#     # else:
#     #     #Write JSON for this product
#     #     metadata={"productname":extracted}
#     #     with open(metf,"w") as f:
#     #         f.write(json.dumps(metadata))
#     # return metf
#
#  def _create_met_json(txt_file, json_file, mis_char):
#      with open(json_file, 'w') as f:
#          json.dump(metadata, f, indent=2)



def exists(url):
    """Check based on protocol if url exists."""

    parsed_url = urlparse(url)
    if parsed_url.scheme == "":
        raise RuntimeError("Invalid url: %s" % url)
    if parsed_url.scheme in ('http', 'https'):
        r = requests.head(url, verify=False)
        if r.status_code == 200:
            return True
        elif r.status_code == 404:
            return False
        else:
            r.raise_for_status()
    elif parsed_url.scheme in ('s3', 's3s'):
        s3_eps = boto.regioninfo.load_regions()['s3']
        region = None
        for r, e in s3_eps.iteritems():
            if re.search(e, parsed_url.netloc):
                region = r
                break
        if region is None:
            raise RuntimeError("Failed to find region for endpoint %s." % \
                               parsed_url.netloc)
        conn = boto.s3.connect_to_region(region,
                                         aws_access_key_id=parsed_url.username,
                                         aws_secret_access_key=parsed_url.password)
        match = re.search(r'/(.*?)/(.*)$', parsed_url.path)
        if not match:
            raise RuntimeError("Failed to parse bucket & key from %s." % \
                               parsed_url.path)
        bn, kn = match.groups()
        try:
            bucket = conn.get_bucket(bn)
        except boto.exception.S3ResponseError, e:
            if e.status == 404:
                return False
            else:
                raise
        key = bucket.get_key(kn)
        if key is None:
            return False
        else:
            return True
    else:
        raise NotImplementedError("Failed to check existence of %s url." % \
                                  parsed_url.scheme)


def sling(download_url, file_type, prod_met=None,
          oauth_url=None):
    """Download file, push to repo and submit job for extraction."""

    # log force flags
    # logging.info("force: %s; force_extract: %s" % (force, force_extract))

    # get localize_url
    # if repo_url.startswith('dav'):
    #     localize_url = "http%s" % repo_url[3:]
    # else:
    #     localize_url = repo_url
    #
    # # get filename
    pri_zip_path = os.path.basename(download_url)

    # check if localize_url already exists
    #    is_here = exists(localize_url)
    # is_here = False
    #   logging.info("%s existence: %s" % (localize_url, is_here))

    # do nothing if not being forced
    #    if is_here and not force and not force_extract: return

    # download from source if not here or forced
    # if not is_here or force:

    # download
    logging.info("Downloading %s to %s." % (download_url, pri_zip_path))
    try:
        osaka.main.get(download_url, pri_zip_path, params={"oauth": oauth_url}, measure=True, output="./pge_metrics.json")
    except Exception, e:
        tb = traceback.format_exc()
        logging.error("Failed to download %s to %s: %s" % (download_url,
                                                           pri_zip_path, tb))
        raise

    # verify downloaded file was not corrupted
    logging.info("Verifying %s is file type %s." % (pri_zip_path, file_type))
    try:
        verify(pri_zip_path, file_type)
        sec_zip_dir = extract(pri_zip_path)
        # remove first zip file
        os.remove(pri_zip_path)

        # unzip the second layer to gather metadata
        sec_zip_file = glob.glob(os.path.join(sec_zip_dir,'*.zip'))
        if not sec_zip_file or len(sec_zip_file) > 1:
            raise RuntimeError("Unable to find second zipfile under " % pri_zip_path)

        logging.info("Verifying %s is file type %s." % (sec_zip_file, file_type))
        verify(sec_zip_file, file_type)
        product= extract(sec_zip_file)

    except Exception, e:
        tb = traceback.format_exc()
        logging.error("Failed to verify and extract files of type %s: %s" % \
                      (file_type, tb))
        raise
    # TODO: move all files forward

    # open summary.txt to extract metadata
    with open(os.path.join(prod_dir, "summary.txt"), 'r') as f:
        # need to add dummy section for config parse to read .properties file
        summary_string = '[summary]\n' + f.read()
    buf = StringIO.StringIO(summary_string)
    config = ConfigParser.ConfigParser()
    config.readfp(buf)

    # parse the metadata
    #TODO: These are hardcoded! Do we need them?
    dataset_id = config.get("summary", "Scs_SceneID")
    prod_date = datetime.strptime(dataset_id[:-6], '%y%m%d').strftime("%Y-%m-%d")
    prod_met['source'] = "jaxa"
    prod_met['dataset_type'] = dataset_id[0:5]
    location =
    prod_met['spatial_extent'] = location


    # Make a product here
    dataset_name = "ALOS2_L1.5_GeoTIFF-" + prod_date + "-" + os.path.basename(product)
    proddir = os.path.join(".", dataset_name)
    os.makedirs(proddir)
    shutil.move(product, proddir)
    metadata = {
        "download_url": download_url,
        "prod_name": dataset_id,
        "prod_date": prod_date,
        "data_product_name": os.path.basename(product),
        "dataset": "ALOS2_L1.5_GeoTIFF",
    }

    # Add metadata from context.json
    if prod_met is not None:
        prod_met = json.loads(prod_met)
        if prod_met:
            metadata.update(prod_met)

    # dump metadata
    with open(os.path.join(proddir, dataset_name + ".met.json"), "w") as f:
        json.dump(metadata, f)
        f.close()

    # get settings
    settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 'settings.json')
    if not os.path.exists(settings_file):
        settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                     'settings.json.tmpl')
    settings = json.load(open(settings_file))

    # dump dataset
    with open(os.path.join(proddir, dataset_name + ".dataset.json"), "w") as f:
        dataset_json = {"version": settings["INCOMING_VERSION"]}
        if 'spatial_extent' in prod_met:
            dataset_json['location'] = prod_met['spatial_extent']
        json.dump(dataset_json, f)
        f.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("download_url", help="download file URL " +
                                             "(credentials stored " +
                                             "in .netrc)")
    # we do not need a repo url
    parser.add_argument("repo_url", help="repository file URL")
    parser.add_argument("prod_name", help="product name to use for " +
                                          " canonical product directory")
    parser.add_argument("file_type", help="download file type to verify",
                        choices=ALL_TYPES)
    parser.add_argument("prod_date", help="product date to use for " +
                                          " canonical product directory")
    parser.add_argument("--oauth_url", help="OAuth authentication URL " +
                                            "(credentials stored in " +
                                            ".netrc)", required=False)

    # we do not have dav, this is meaningless to us
    # group = parser.add_mutually_exclusive_group()
    # group.add_argument("-f", "--force", help="force download from source, " +
    #                                          "upload to repository, and " +
    #                                          "extract-ingest job " +
    #                                          "submission; by default, " +
    #                                          "nothing is done if the " +
    #                                          "repo_url exists",
    #                    action='store_true')
    # group.add_argument("-e", "--force_extract", help="force extract-ingest " +
    #                                                  "job submission; if repo_url " +
    #                                                  "exists, skip download from " +
    #                                                  "source and use whatever is " +
    #                                                  "at repo_url", action='store_true')
    args = parser.parse_args()
    # load prod_met as string

    j = json.loads(open("_context.json", "r").read())
    prod_met = json.dumps(j["prod_met"])

    try:
        sling(args.download_url, args.prod_name, args.file_type,
              args.prod_date, prod_met, args.oauth_url)
    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
