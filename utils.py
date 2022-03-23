
from datetime import datetime
from tempfile import TemporaryFile
from time import sleep
import traceback

freq_error = 10


def truncate_text(txt, length):
	try:
		return txt if len(txt) <= length else txt[:length]
	except:
		return None


def ns():  # XML namespace
	return {
		'ns3' : 'http://zakupki.gov.ru/oos/common/1',
		'ns4' : 'http://zakupki.gov.ru/oos/base/1',
		'exp': 'http://zakupki.gov.ru/oos/export/1',
		's': 'http://zakupki.gov.ru/oos/EPtypes/1',
		'int': 'http://zakupki.gov.ru/oos/integration/1',
		'print': 'http://zakupki.gov.ru/oos/printform/1'
	}


def ts():  # return timestamp
	return '[' + str(datetime.now().replace(microsecond=0)) + ']'


# Here folder filter is used - to only parse region folders and not some garbage folders.
# Is done through presense of _obl or _Aobl or _Resp or _kraj or _AO or _g in the end of folder names;
# also folders Moskva/ and Sankt-Peterburg/ shouldnt be forgotten.
# All regions are parsed - which can take up to weeks - unless arguments in etl function are used.
def get_regions(ftp, regions_start=None, regions_end=None):
	ftp.cwd('/fcs_regions')
	regions = []
	regions.extend(ftp.nlst('*_obl'))
	regions.extend(ftp.nlst('*_Resp'))
	regions.extend(ftp.nlst('*_kraj'))
	regions.extend(ftp.nlst('*_AO'))
	regions.extend(ftp.nlst('*_g'))
	regions.extend(ftp.nlst('*_Aobl'))
	regions.extend(['Sankt-Peterburg', 'Moskva'])

	regions_start = 0 if not regions_start else regions_start
	regions_start = 0 if regions_start < 0 else regions_start
	regions_end = len(regions) if not regions_end else regions_end
	regions_end = len(regions) if regions_end > len(regions) else regions_end
	regions = regions[regions_start: regions_end]

	return regions


def nlst(ftp, mask, retry=10):
	try:
		return ftp.nlst(mask)
	except:
		if retry > 0:
			return nlst(ftp, mask, retry - 1)
		else:
			return None


def ftp_size(ftp, files):
	s = 0
	for f in files:
		s += ftp.size(f)
	return float(s)


def retr(ftp, path, retry=6):  # retrieve file via FTP and return
	tmp = TemporaryFile()
	cur_retry = retry
	while cur_retry > 0:
		try:
			size = ftp.size(path)
			ftp.retrbinary('RETR ' + path, tmp.write)
			assert size == tmp.tell()
			return tmp
		except:
			if cur_retry > 0:
				tmp.close()
				if cur_retry == retry // 2:
					sleep(freq_error)  # waiting in case server is not responding
				cur_retry -= 1
			else:
				tmp.close()
				return None


def retrieve(xml, xpath, fun=lambda x: x):
	try:
		ans = xml.xpath(xpath, namespaces=ns(), smart_strings=False)
		return fun(ans[0])
	except:
		# traceback.print_exc()
		return None


def parse_datetime(date):
	return datetime.strptime(date[:19], '%Y-%m-%dT%H:%M:%S')


def parse_date(date):
	return datetime.strptime(date, '%Y-%m-%d')


# inc currently doesnt work
def inc_files(coll, ftp, folder_name):
	files = {}
	cur_dir = '/fcs_regions/{region}/{collection}'.format(region=folder_name, collection=coll)
	ftp.cwd(cur_dir)
	files[cur_dir] = nlst(ftp, '*.xml.zip')
	cur_dir = '/fcs_regions/{region}/{collection}/prevMonth'.format(region=folder_name, collection=coll)
	ftp.cwd(cur_dir)
	files[cur_dir] = nlst(ftp, '*.xml.zip')
	cur_dir = '/fcs_regions/{region}/{collection}/currMonth'.format(region=folder_name, collection=coll)
	ftp.cwd(cur_dir)
	files[cur_dir] = nlst(ftp, '*.xml.zip')
	return files


def all_files(coll, ftp, folder_name):  # list all files for this region / collection
	files = {}
	cur_dir = '/fcs_regions/{region}/{collection}'.format(region=folder_name, collection=coll)
	ftp.cwd(cur_dir)
	files[cur_dir] = nlst(ftp, '*.xml.zip')
	cur_dir = '/fcs_regions/{region}/{collection}/prevMonth'.format(region=folder_name, collection=coll)
	ftp.cwd(cur_dir)
	files[cur_dir] = nlst(ftp, '*.xml.zip')
	cur_dir = '/fcs_regions/{region}/{collection}/currMonth'.format(region=folder_name, collection=coll)
	ftp.cwd(cur_dir)
	files[cur_dir] = nlst(ftp, '*.xml.zip')
	return files
