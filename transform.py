from utils import *
from sql import *

from numpy import unique


def transform_notifications(xml, region_id):
	document = dict()

	document['finSource'] = retrieve(xml, './s:lot/s:financeSource/text()', str)
	if document['finSource']:
		document['finSource'].strip().strip('.').lower()
	document['finSource'] = truncate_text(document['finSource'], 1000)
	try:
		fin_id, = one_row_request("INSERT INTO finance_sources (source) VALUES (%s) RETURNING id;",
								  [document['finSource']], if_commit=True)
	except psycopg2.IntegrityError:
		fin_id, = one_row_request("SELECT id from finance_sources WHERE source = %s;", [document['finSource']])

	document['procurerRegNum'] = retrieve(xml, './s:purchaseResponsible/s:responsibleOrg/s:regNum/text()', str)
	document['procurerName'] = retrieve(xml, './s:purchaseResponsible/s:responsibleOrg/s:fullName/text()', str)
	document['procurerName'] = truncate_text(document['procurerName'], 1000)
	document['procurerINN'] = retrieve(xml, './s:purchaseResponsible/s:responsibleOrg/s:INN/text()', str)
	no_return_command("INSERT INTO procurers (reg_num, INN, name) VALUES (%s, %s, %s) ON CONFLICT (reg_num) "
					  "DO NOTHING;", [document['procurerRegNum'], document['procurerINN'], document['procurerName']])

	document['purchaseNumber'] = retrieve(xml, './s:purchaseNumber/text()', str)
	document['startDate'] = retrieve(xml, './s:procedureInfo/s:collecting/s:startDate/text()', parse_datetime)
	document['endDate'] = retrieve(xml, './s:procedureInfo/s:collecting/s:endDate/text()', parse_datetime)
	document['maxPrice'] = retrieve(xml, './s:lot/s:maxPrice/text()', lambda x: round(float(x), 2))
	document['currency'] = retrieve(xml, './s:lot/s:maxPrice/s:currency/s:code/text()', str)

	document['deliveryTerm'] = retrieve(xml, './s:lot/s:customerRequirements/s:customerRequirement')
	document['deliveryTerm'] = retrieve(document['deliveryTerm'], './s:deliveryTerm/text()', str)
	document['deliveryTerm'] = truncate_text(document['deliveryTerm'], 200)
	document['electronic'] = False

	auction_id, = one_row_request("INSERT INTO auctions (region_id, purchase_number, start_date, end_date, max_price, "
								  "currency, procurer_reg_num, finance_source_id, delivery_term) "
								  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (purchase_number) "
								  "DO UPDATE SET start_date = %s, end_date = %s, max_price = %s, "
								  "currency = %s, procurer_reg_num = %s, finance_source_id = %s, "
								  "delivery_term = %s, electronic = %s RETURNING id;",
								  [region_id, document['purchaseNumber'], document['startDate'], document['endDate'],
								   document['maxPrice'], document['currency'], document['procurerRegNum'], fin_id,
								   document['deliveryTerm'], document['startDate'], document['endDate'],
								   document['maxPrice'], document['currency'], document['procurerRegNum'], fin_id,
								   document['deliveryTerm'], document['electronic']], if_commit=True)  # index out of range

	if retrieve(xml, './s:drugPurchaseObjectInfo'):
		objects = [['00', '00', '00', '000']]
		obj_names = ['Drugs']
		if_okpd2 = [False]
	else:
		objects = []
		obj_names = []
		if_okpd2 = []
	for obj_xml in xml.xpath('./s:lot/s:purchaseObjects/s:purchaseObject', namespaces=ns()):
		okpd2_code = retrieve(obj_xml, './s:OKPD2/s:code/text()', str)
		if okpd2_code is not None:
			okpd2_name = retrieve(obj_xml, './s:OKPD2/s:name/text()', str)
			if_okpd2.append(True)
		else:
			okpd2_code = retrieve(obj_xml, './s:OKPD/s:code/text()', str)
			okpd2_name = retrieve(obj_xml, './s:OKPD/s:name/text()', str)
			if_okpd2.append(False)
		if okpd2_code is not None:
			objects.append(okpd2_code)
			okpd2_name = truncate_text(okpd2_name, 1000)
			obj_names.append(okpd2_name)
		else:
			if_okpd2.pop()

	if len(objects) > 0:
		objects, indices = unique(objects, return_index=True)
	for i, obj in enumerate(objects):
		obj = obj.split('.')
		while len(obj) < 4:
			obj.append('00')
		obj_name = obj_names[indices[i]]
		if_okpd2_cur = if_okpd2[indices[i]]
		try:
			obj_id, = one_row_request("INSERT INTO purchase_objects (code_1, code_2, code_3, code_4, if_OKPD2, "
									  "name) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
									  [obj[0], obj[1], obj[2], obj[3], if_okpd2_cur, obj_name], if_commit=True)
		except psycopg2.IntegrityError:
			obj_id, = one_row_request("SELECT id FROM purchase_objects WHERE name = %s;", [obj_name])
		no_return_command("INSERT INTO auction_purchase_objects (auction_id, purchase_object_id) "
						  "VALUES (%s, %s) ON CONFLICT DO NOTHING;", [auction_id, obj_id])  # index out of range


def transform_notifications_el(xml, region_id):
	document = dict()
	#print(xml)
	document['finSource'] = retrieve(xml, './s:notificationInfo/s:customerRequirementsInfo/s:customerRequirementInfo/s:contractConditionsInfo/s:contractExecutionPaymentPlan/s:financingSourcesInfo	/s:financingSource/text()', str)
	if document['finSource']:
		document['finSource'].strip().strip('.').lower()
	document['finSource'] = truncate_text(document['finSource'], 1000)
	try:
		fin_id, = one_row_request("INSERT INTO finance_sources (source) VALUES (%s) RETURNING id;",
								  [document['finSource']], if_commit=True)
	except psycopg2.IntegrityError:
		fin_id, = one_row_request("SELECT id from finance_sources WHERE source = %s;", [document['finSource']])

	document['procurerRegNum'] = retrieve(xml, './s:purchaseResponsibleInfo/s:responsibleOrgInfo/s:regNum/text()', str)
	document['procurerName'] = retrieve(xml, './s:purchaseResponsibleInfo/s:responsibleOrgInfo/s:fullName/text()', str)
	document['procurerName'] = truncate_text(document['procurerName'], 1000)
	document['procurerINN'] = retrieve(xml, './s:purchaseResponsibleInfo/s:responsibleOrgInfo/s:INN/text()', str)
	no_return_command("INSERT INTO procurers (reg_num, INN, name) VALUES (%s, %s, %s) ON CONFLICT (reg_num) "
					  "DO NOTHING;", [document['procurerRegNum'], document['procurerINN'], document['procurerName']])

	document['purchaseNumber'] = retrieve(xml, './s:commonInfo/s:purchaseNumber/text()', str)
	document['startDate'] = retrieve(xml, './s:notificationInfo/s:procedureInfo/s:collectingInfo/s:startDT/text()', parse_datetime)
	document['endDate'] = retrieve(xml, './s:notificationInfo/s:procedureInfo/s:collectingInfo/s:endDT/text()', parse_datetime)
	document['maxPrice'] = retrieve(xml, './s:notificationInfo/s:contractConditionsInfo/s:maxPriceInfo/s:maxPrice/text()', lambda x: round(float(x), 2))
	document['currency'] = retrieve(xml, './s:notificationInfo/s:contractConditionsInfo/s:maxPriceInfo/s:currency/ns4:code/text()', str)
	
	document['deliveryTerm'] = retrieve(xml, './s:notificationInfo/s:customerRequirementsInfo/s:customerRequirementInfo/s:contractConditionsInfo')
	document['deliveryTerm'] = retrieve(document['deliveryTerm'], './s:deliveryTerm/text()', str)
	document['deliveryTerm'] = truncate_text(document['deliveryTerm'], 200)
	document['electronic'] = True
	document['object'] = retrieve(xml, './s:commonInfo/s:purchaseObjectInfo/text()', str)

	auction_id, = one_row_request("INSERT INTO auctions (region_id, purchase_number, start_date, end_date, max_price, "
								  "currency, procurer_reg_num, finance_source_id, delivery_term) "
								  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (purchase_number) "
								  "DO UPDATE SET start_date = %s, end_date = %s, max_price = %s, "
								  "currency = %s, procurer_reg_num = %s, finance_source_id = %s, "
								  "delivery_term = %s, electronic = %s RETURNING id;",
								  [region_id, document['purchaseNumber'], document['startDate'], document['endDate'],
								   document['maxPrice'], document['currency'], document['procurerRegNum'], fin_id,
								   document['deliveryTerm'], document['startDate'], document['endDate'],
								   document['maxPrice'], document['currency'], document['procurerRegNum'], fin_id,
								   document['deliveryTerm'], document['electronic']], if_commit=True)  # index out of range

	if retrieve(xml, './s:notificationInfo/s:purchaseObjectsInfo/ns3:drugPurchaseObjectInfo'):
		objects = [['00', '00', '00', '000']]
		obj_names = ['Drugs']
		if_okpd2 = [False]
	else:
		objects = []
		obj_names = []
		if_okpd2 = []
	for obj_xml in xml.xpath('./s:notificationInfo/s:purchaseObjectsInfo/s:notDrugPurchaseObjectsInfo/ns3:purchaseObject', namespaces=ns()):
		okpd2_code = retrieve(obj_xml, './ns3:OKPD2/ns4:OKPDCode/text()', str)
		if okpd2_code is not None:
			okpd2_name = retrieve(obj_xml, './ns3:OKPD2/ns4:OKPDName/text()', str)
			if_okpd2.append(True)
		else:
			okpd2_code = retrieve(obj_xml, './s:OKPD/s:OKPDCode/text()', str)
			okpd2_name = retrieve(obj_xml, './s:OKPD/s:OKPDName/text()', str)
			if_okpd2.append(False)
		if okpd2_code is not None:
			objects.append(okpd2_code)
			okpd2_name = truncate_text(okpd2_name, 1000)
			obj_names.append(okpd2_name)
		else:
			if_okpd2.pop()

	if len(objects) > 0:
		objects, indices = unique(objects, return_index=True)
	for i, obj in enumerate(objects):
		obj = obj.split('.')
		while len(obj) < 4:
			obj.append('00')
		obj_name = obj_names[indices[i]]
		if_okpd2_cur = if_okpd2[indices[i]]
		try:
			obj_id, = one_row_request("INSERT INTO purchase_objects (code_1, code_2, code_3, code_4, if_OKPD2, "
									  "name, object) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;",
									  [obj[0], obj[1], obj[2], obj[3], if_okpd2_cur, obj_name, document['object']], if_commit=True)
		except psycopg2.IntegrityError:
			obj_id, = one_row_request("SELECT id FROM purchase_objects WHERE name = %s;", [obj_name])
		no_return_command("INSERT INTO auction_purchase_objects (auction_id, purchase_object_id) "
						  "VALUES (%s, %s) ON CONFLICT DO NOTHING;", [auction_id, obj_id])  # index out of range


def transform_notifications_prolong(xml, region_id):
	purchase_number = retrieve(xml, './s:purchaseNumber/text()', str)
	prolong_date = retrieve(xml, './s:collectingProlongationDate/text()', parse_datetime)
	no_return_command("INSERT INTO auctions (region_id, purchase_number, prolong_date) "
					  "VALUES (%s, %s, %s) ON CONFLICT (purchase_number) DO UPDATE SET "
					  "prolong_date = %s;",
					  [region_id, purchase_number, prolong_date, prolong_date])


def transform_protocols(xml, region_id, if_prolong=False):
	purchase_number = retrieve(xml, './s:purchaseNumber/text()', str)
	n_commission_members = len(xml.xpath('./s:commission/s:commissionMembers/s:commissionMember', namespaces=ns()))
	auction_id, = one_row_request("INSERT INTO auctions (region_id, purchase_number, n_commission_members) "
								  "VALUES (%s, %s, %s) ON CONFLICT (purchase_number) DO UPDATE "
								  "SET n_commission_members = %s RETURNING id;",
								  [region_id, purchase_number, n_commission_members, n_commission_members],
								  if_commit=True)
	
	for i, application_xml in enumerate(xml.xpath('./s:protocolLot/s:applications/s:application', namespaces=ns())):
		bid_price = retrieve(application_xml, './s:price/text()', lambda x: round(float(x), 2))
		bid_time = retrieve(application_xml, './s:appDate/text()', parse_datetime)
		part_inn = retrieve(application_xml, './s:appParticipant/s:inn/text()', str)
		part_name = retrieve(application_xml, './s:appParticipant/s:organizationName/text()', str)
		part_name = truncate_text(part_name, 1000)
		
		correspondences = application_xml.xpath('./s:correspondencies/s:correspondence', namespaces=ns())
		if i == 0:
			is_approved = parse_correspondences(auction_id, correspondences)
					
		else:
			is_approved = True
			for correspondence in correspondences:
				compatible = True if retrieve(correspondence, './s:compatible/text()', str) == 'true' else False
				if not compatible:
					is_approved = False
					break
		
		no_return_command("INSERT INTO participants (INN, name) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
						  [part_inn, part_name])
		no_return_command("INSERT INTO bids (auction_id, participant_INN, price, date, is_approved, is_after_prolong) "
						  "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
						  [auction_id, part_inn, bid_price, bid_time, is_approved, if_prolong])


def transform_protocols_el(xml, region_id, if_prolong=False):
	purchase_number = retrieve(xml, './s:commonInfo/s:purchaseNumber/text()', str)
	#print(purchase_number)
	n_commission_members = len(xml.xpath('./s:protocolInfo/s:commissionInfo/ns3:commissionMembers/ns3:commissionMember', namespaces=ns()))
	auction_id, = one_row_request("INSERT INTO auctions (region_id, purchase_number, n_commission_members) "
								  "VALUES (%s, %s, %s) ON CONFLICT (purchase_number) DO UPDATE "
								  "SET n_commission_members = %s RETURNING id;",
								  [region_id, purchase_number, n_commission_members, n_commission_members],
								  if_commit=True)

	coord = {}
	for i, application_xml in enumerate(xml.xpath('./s:protocolInfo/s:applicationsInfo/s:applicationInfo', namespaces=ns())):
		bid_price = retrieve(application_xml, './s:finalPrice/text()', lambda x: round(float(x), 2))
		bid_time = retrieve(application_xml, './s:commonInfo/s:appDT/text()', parse_datetime)
		appNumber = retrieve(application_xml, './s:commonInfo/s:appNumber/text()', str)


		correspondences = application_xml.xpath('./s:commonInfo/s:admissionResultsInfo/s:admissionResultInfo', namespaces=ns())
		if i == 0:
			is_approved = parse_correspondences(auction_id, correspondences)

		else:
			is_approved = True
			for correspondence in correspondences:
				compatible = True if retrieve(correspondence, './s:admitted/text()', str) == 'true' else False
				if not compatible:
					is_approved = False
					break

		coord[appNumber] = {'bid_price': bid_price, 'bid_time':bid_time, 'is_approved': is_approved}

	for i, application_xml in enumerate(xml.xpath('./s:appParticipantsInfo/s:appParticipantInfo', namespaces=ns())):
		app_number = retrieve(application_xml, './s:appNumber/text()', str)
		part_inn = retrieve(application_xml, './s:participantInfo/ns3:legalEntityRFInfo/ns3:INN/text()', str)
		part_name = retrieve(application_xml, './s:participantInfo/ns3:legalEntityRFInfo/ns3:fullName/text()', str)
		if not part_inn:
			part_inn = retrieve(application_xml, './s:participantInfo/ns3:individualPersonRFInfo/ns3:INN/text()',
								str)

		if not part_name:
			part_name = retrieve(application_xml,
								 './s: p	articipantInfo/ns3:individualPersonRFInfo/ns3:nameInfo/ns3:lastName/text()',
								 str)
		part_name = truncate_text(part_name, 1000)

		bid_time = coord[app_number]['bid_time']
		bid_price = coord[app_number]['bid_price']
		is_approved = coord[app_number]['is_approved']
		no_return_command("INSERT INTO participants (INN, name) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
						  [part_inn, part_name])
		no_return_command("INSERT INTO bids (auction_id, participant_INN, price, date, is_approved, is_after_prolong) "
						  "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
						  [auction_id, part_inn, bid_price, bid_time, is_approved, if_prolong])


def parse_correspondences(auction_id, correspondences):
	is_approved = True
	for correspondence in correspondences:
		if is_approved:
			compatible = True if retrieve(correspondence, './s:admitted/text()', str) == 'true' else False
			if not compatible:
				is_approved = False

		requirement = retrieve(correspondence, './s:requirement')
		if requirement is None:
			requirement = retrieve(correspondence, './s:preferense')
		if requirement is None:
			requirement = retrieve(correspondence, './s:restriction')
		req_code = retrieve(requirement, './s:code/text()', str)
		req_name = retrieve(requirement, './s:name/text()', str)
		req_name = truncate_text(req_name, 1000)
		req_id = None

		# correspondences info probably should have been taken from notifications, but it's not a big deal
		if req_code is None:
			req_short_name = retrieve(requirement, './s:shortName/text()', str)
			if req_short_name:
				req_id, = one_row_request("INSERT INTO correspondences (name, short_name) "
										  "VALUES (%s, %s) ON CONFLICT (name) "
										  "DO UPDATE SET short_name = %s RETURNING id;",
										  [req_name, req_short_name, req_short_name], if_commit=True)
		else:
			req_id, = one_row_request("INSERT INTO correspondences (name, code) "
									  "VALUES (%s, %s) ON CONFLICT (name) DO UPDATE SET code = %s RETURNING id;",
									  [req_name, req_code, req_code], if_commit=True)

		if req_id:
			no_return_command("INSERT INTO auction_correspondences (auction_id, correspondence_id) "
							  "VALUES (%s, %s) ON CONFLICT DO NOTHING;", [auction_id, req_id])
		return is_approved
