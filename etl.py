from lxml import etree
from zipfile import ZipFile
from tqdm import tqdm
import traceback

from transform import *
from utils import *
from sql import *


def etl(ftp, coll, update_type, regions=None, regions_start=None, regions_end=None):
    """
        regions - list of regions to parse
        regions_start, regions_end - borders that limit list of parsed regions
    """
    ftp.cwd('/fcs_regions')
    if not regions:
        regions = get_regions(ftp, regions_start, regions_end)

    print(ts(), 'regions: {regions}'.format(regions=regions))
    files = {}
    total_size = 0.0
    n_files = 0
    for region in regions:
        region_files = inc_files(coll, ftp, region) if update_type == 'inc' else all_files(coll, ftp, region)
        size = 0.0
        for dr, fs in region_files.items():
            size += ftp_size(ftp, [dr + '/' + f for f in fs])
            n_files += len(fs)
        files[region] = region_files
        print(ts(), region, round(size / (1024 * 1024), 2), 'Mb')
        total_size += size

    print(ts(), 'Loading {len} files, {size} Mb total'.format(len=n_files, size=round(total_size / (1024 * 1024), 2)))
    ftp.cwd('/')
    for region, region_dicts in files.items():
        try:
            region_id, = one_row_request("INSERT INTO regions (region) VALUES (%s) RETURNING id;",
                                         [region], if_commit=True)
        except psycopg2.IntegrityError:
            region_id, = one_row_request("SELECT id FROM regions WHERE region = %s;", [region])
        print(ts(), region)
        for directory, region_files in region_dicts.items():
            ftp.cwd(directory)

            for f in tqdm(region_files):
                if 'protocol_{}_2014'.format(region) in f or 'protocol_{}_2015'.format(region) in f or 'protocol_{}_2016'.format(region) in f or 'protocol_{}_2017'.format(region) in f:  # skip unnecessary years
                    print(f)
                    continue
                if 'notification_{}_2014'.format(region) in f or 'notification_{}_2015'.format(region) in f or 'notification_{}_2016'.format(region) in f or 'notification_{}_2017'.format(region) in f:  # skip unnecessary years
                    continue
                try:
                    zip_file = retr(ftp, f)
                    assert zip_file
                    zip_file = ZipFile(zip_file)
                    zip_names = zip_file.namelist()
                except Exception:
                    print('\nSkip file {file}'.format(file=f))
                    continue

                for name in zip_names:
                    parse_xml(zip_file, name, region_id)
                zip_file.close()


# print("regions have been downloaded:\n{regions}".format(regions=regions))
def parse_xml(zip_file, name, region_id):
    try:
        if name.startswith('fcsNotificationZK')  or name.startswith('fksNotificationZK'):
            xml_file = zip_file.open(name)
            parsed = etree.iterparse(xml_file, tag='{http://zakupki.gov.ru/oos/export/1}fcsNotificationZK')
            if parsed:
                for event, xml in parsed:
                    if event == 'end':
                        transform_notifications(xml, region_id)
                        return
            else:
                parsed = etree.iterparse(xml_file, tag='{http://zakupki.gov.ru/oos/export/1}epNotificationEZK')
                #   print(parsed)
                for event, xml in parsed:
                    #  print(xml, event)
                    if event == 'end':
                        transform_notifications_el(xml, region_id)
                        return

        elif name.startswith('epNotificationEZK'):
            xml_file = zip_file.open(name)
            # print(xml_file)
            parsed = etree.iterparse(xml_file, tag='{http://zakupki.gov.ru/oos/export/1}epNotificationEZK2020')
            #   print(parsed)
            for event, xml in parsed:
                #  print(xml, event)
                if event == 'end':
                    transform_notifications_el(xml, region_id)
                    return

        elif name.startswith('fcsPurchaseProlongationZK'):
            xml_file = zip_file.open(name)
            parsed = etree.iterparse(xml_file, tag='{http://zakupki.gov.ru/oos/export/1}fcsPurchaseProlongationZK')
            for event, xml in parsed:
                if event == 'end':
                    transform_notifications_prolong(xml, region_id)
                    return

        elif name.startswith('fcsProtocolZKAfterProlong'):
            xml_file = zip_file.open(name)
            parsed = etree.iterparse(xml_file, tag='{http://zakupki.gov.ru/oos/export/1}fcsProtocolZKAfterProlong')
            for event, xml in parsed:
                if event == 'end':
                    transform_protocols(xml, region_id, if_prolong=True)
                    return

        elif name.startswith('fcsProtocolZK'):
            xml_file = zip_file.open(name)
            parsed = etree.iterparse(xml_file, tag='{http://zakupki.gov.ru/oos/export/1}fcsProtocolZK')
            for event, xml in parsed:
                if event == 'end':
                    transform_protocols(xml, region_id, if_prolong=False)
                    return

        elif name.startswith('epProtocolEZK2020FinalPart') or name.startswith('fcsProtocolPRO_'):
            xml_file = zip_file.open(name)
            parsed = etree.iterparse(xml_file, tag='{http://zakupki.gov.ru/oos/export/1}epProtocolEZK2020FinalPart')

            for event, xml in parsed:
                if event == 'end':
                    transform_protocols_el(xml, region_id, if_prolong=False)
                    return

    except etree.XMLSyntaxError:
        return
    except Exception:
        traceback.print_exc()
        return

