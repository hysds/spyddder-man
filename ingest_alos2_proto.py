#!/usr/bin/env python
"""
Ingest ALOS2 data from a source to a destination:

  1) download data from a source and verify,
  2) extracts data and creates metadata
  3) push data to repository


HTTP/HTTPS, FTP and OAuth authentication is handled using .netrc.
"""

import datetime, os, sys, re, requests, json, logging, traceback, argparse, shutil, glob
import zipfile
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

from subprocess import check_call

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
        prod_dir = zip_file.replace(".zip", "")
        zf.extractall(prod_dir)
    return prod_dir


def download(download_url, dest, oauth_url):
    # download
    logging.info("Downloading %s to %s." % (download_url, dest))
    try:
        osaka.main.get(download_url, dest, params={"oauth": oauth_url}, measure=True, output="./pge_metrics.json")
    except Exception, e:
        tb = traceback.format_exc()
        logging.error("Failed to download %s to %s: %s" % (download_url,
                                                           dest, tb))
        raise


def create_metadata(alos2_md_file, download_url):
    # TODO: Some of these are hardcoded! Do we need them?
    metadata = {}
    # open summary.txt to extract metadata
    # extract information from summary see: https://www.eorc.jaxa.jp/ALOS-2/en/doc/fdata/PALSAR-2_xx_Format_GeoTIFF_E_r.pdf
    logging.info("Extracting metadata from %s" % alos2_md_file)
    dummy_section = "summary"
    with open(alos2_md_file, 'r') as f:
        # need to add dummy section for config parse to read .properties file
        summary_string = '[%s]\n' % dummy_section + f.read()
    summary_string = summary_string.replace('"', '')
    buf = StringIO.StringIO(summary_string)
    config = ConfigParser.ConfigParser()
    config.readfp(buf)

    # parse the metadata from summary.txt
    alos2md = {}
    for name, value in config.items(dummy_section):
        alos2md[name] = value

    metadata['alos2md'] = alos2md

    # facetview filters
    dataset_name = metadata['alos2md']['scs_sceneid'] + "_" + metadata['alos2md']['pds_productid']
    metadata['prod_name'] = dataset_name
    metadata['spacecraftName'] = dataset_name[0:5]
    metadata['dataset_type'] = dataset_name[0:5]
    metadata['orbitNumber'] = int(dataset_name[5:10])
    metadata['scene_frame_number'] = int(dataset_name[10:14])
    prod_datetime = datetime.datetime.strptime(dataset_name[15:21], '%y%m%d')
    prod_date = prod_datetime.strftime("%Y-%m-%d")
    metadata['prod_date'] = prod_date

    # TODO: not sure if this is the right way to expose this in Facet Filters, using CSK's metadata structure
    dfdn = {"AcquistionMode": dataset_name[22:25],
            "LookSide": dataset_name[25]}
    metadata['dfdn'] = dfdn

    metadata['lookDirection'] = "right" if dataset_name[25] is "R" else "left"
    metadata['level'] = "L" + dataset_name[26:29]
    metadata['processingOption'] = dataset_name[29]
    metadata['mapProjection'] = dataset_name[30]
    metadata['direction'] = "ascending" if dataset_name[31] is "A" else "descending"

    # others
    metadata['dataset'] = "ALOS2_GeoTIFF"
    metadata['source'] = "jaxa"
    metadata['download_url'] = download_url
    location = {}
    location['type'] = 'Polygon'
    location['coordinates'] = [[
        [float(metadata['alos2md']['img_imagescenelefttoplongitude']), float(metadata['alos2md']['img_imagescenelefttoplatitude'])],
        [float(metadata['alos2md']['img_imagescenerighttoplongitude']), float(metadata['alos2md']['img_imagescenerighttoplatitude'])],
        [float(metadata['alos2md']['img_imagescenerightbottomlongitude']), float(metadata['alos2md']['img_imagescenerightbottomlatitude'])],
        [float(metadata['alos2md']['img_imagesceneleftbottomlongitude']), float(metadata['alos2md']['img_imagesceneleftbottomlatitude'])],
        [float(metadata['alos2md']['img_imagescenelefttoplongitude']), float(metadata['alos2md']['img_imagescenelefttoplatitude'])]

    ]]
    metadata['location'] = location

    # # Add metadata from context.json
    # # copy _context.json if it exists
    # ctx = {}
    # ctx_file = "_context.json"
    # if os.path.exists(ctx_file):
    #     with open(ctx_file) as f:
    #         ctx = json.load(f)

    return metadata


def create_dataset(metadata):
    logging.info("Extracting datasets from metadata")
    # get settings for dataset version
    settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 'settings.json')
    if not os.path.exists(settings_file):
        settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                     'settings.json.tmpl')
    settings = json.load(open(settings_file))

    # datasets.json
    # extract metadata for datasets
    dataset = {
        'version': settings['ALOS2_INGEST_VERSION'],
        'label': metadata['prod_name'],
        'starttime': datetime.datetime.strptime(metadata['alos2md']['img_scenestartdatetime'], '%Y%m%d %H:%M:%S.%f').strftime("%Y-%m-%dT%H:%M:%S.%f"),
        'endtime': datetime.datetime.strptime(metadata['alos2md']['img_sceneenddatetime'], '%Y%m%d %H:%M:%S.%f').strftime("%Y-%m-%dT%H:%M:%S.%f")
    }
    dataset['location'] = metadata['location']

    return dataset


def gdal_translate(outfile, infile, options_string):
    cmd = "gdal_translate {} {} {}".format(options_string, infile, outfile)
    return check_call(cmd,  shell=True)


def create_product_browse(tiff_file):
    # TODO: the static scale of 7500 has been chosen! We need better means to scale it.
    logging.info("Creating browse png from %s" % tiff_file)
    options_string = '-of PNG -ot Byte -scale 0 7500 0 255 -outsize 10% 10%'
    out_file = os.path.splitext(tiff_file)[0] + '.browse.png'
    gdal_translate(out_file, tiff_file, options_string)
    return

def create_product_kmz(tiff_file, dataset_name):
    # TODO: the static scale of 7500 has been chosen! We need better means to scale it.
    logging.info("Creating KMZ from %s" % tiff_file)

    options_string = '-of KMLSUPEROVERLAY -ot Byte -scale 0 7500 0 255'
    out_file = dataset_name + ".kmz"
    gdal_translate(out_file, tiff_file, options_string)
    return


def ingest_alos2(download_url, file_type, oauth_url=None):
    """Download file, push to repo and submit job for extraction."""

    # get filename
    pri_zip_path = os.path.basename(download_url)
    download(download_url, pri_zip_path, oauth_url)

    # verify downloaded file was not corrupted
    logging.info("Verifying %s is file type %s." % (pri_zip_path, file_type))
    try:
        verify(pri_zip_path, file_type)
        sec_zip_dir = extract(pri_zip_path)

        # unzip the second layer to gather metadata
        sec_zip_file = glob.glob(os.path.join(sec_zip_dir,'*.zip'))
        if not len(sec_zip_file) == 1:
            raise RuntimeError("Unable to find second zipfile under %s" % sec_zip_dir)

        logging.info("Verifying %s is file type %s." % (sec_zip_file[0], file_type))
        verify(sec_zip_file[0], file_type)
        product_dir = extract(sec_zip_file[0])

    except Exception, e:
        tb = traceback.format_exc()
        logging.error("Failed to verify and extract files of type %s: %s" % \
                      (file_type, tb))
        raise

    # create met.json
    alos2_md_file = os.path.join(product_dir, "summary.txt")
    metadata = create_metadata(alos2_md_file, download_url)

    # create dataset.json
    dataset = create_dataset(metadata)

    # create the product directory
    dataset_name = metadata['prod_name']
    proddir = os.path.join(".", dataset_name)
    os.makedirs(proddir)
    # move all files forward
    files = os.listdir(product_dir)
    for f in files:
        shutil.move(os.path.join(product_dir, f), proddir)

    # dump metadata
    with open(os.path.join(proddir, dataset_name + ".met.json"), "w") as f:
        json.dump(metadata, f, indent=2)
        f.close()

    # dump dataset
    with open(os.path.join(proddir, dataset_name + ".dataset.json"), "w") as f:
        json.dump(dataset, f, indent=2)
        f.close()

    # create browse products
    tiff_files = glob.glob(os.path.join(proddir, "*.tif"))
    for tif_file in tiff_files:
        create_product_browse(tif_file)

    # create kmz products
    for tif_file in tiff_files:
        if "IMG-HH" in tif_file:
            create_product_kmz(tif_file, dataset_name)

    # remove unwanted zips
    shutil.rmtree(sec_zip_dir, ignore_errors=True)
    os.remove(pri_zip_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("download_url", help="download file URL " +
                                             "(credentials stored " +
                                             "in .netrc)")
    parser.add_argument("file_type", help="download file type to verify",
                        choices=ALL_TYPES)
    parser.add_argument("--oauth_url", help="OAuth authentication URL " +
                                            "(credentials stored in " +
                                            ".netrc)", required=False)

    args = parser.parse_args()

    try:
        ingest_alos2(args.download_url, args.file_type, oauth_url=args.oauth_url)
    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
