import pandas as pd
import numpy as np
import openpyxl
import os.path
from openpyxl.utils.dataframe import dataframe_to_rows
import pprint
import json
import plenty_api

CATEGORY_ID_MAPPING = {
    '34' : '10060201',
    '35' : '10060201',
    '36' : '10060201',
    '38' : '10040301',
    '39' : '100A0301',
    '40' : '10070401',
    '41' : '10010D01',
    '53' : '10060201',
    '62' : '10080101',
    '63' : '10040101',
    '64' : '100C0201',
    '65' : '100A0201',
    '66' : '100A0301',
    '68' : '10070401',
    '69' : '100C0201',
    '70' : '10060201',
    '71' : '10060201',
    '72' : '10060201',
    '73' : '10060501',
    '84' : '10010D01',
    '85' : '10060201',
    '86' : '10060201',
    '87' : '10080201'
}

MARKETING_COLOR_MAPPING = {
    '415' : 'Bleu jean',
    '234' : 'Mauve',
    '160' : 'Rose',
    '117' : 'Rouge tomate',
    '345' : 'Bleu turquoise',
    '205' : 'Bleu',
    '193' : 'Vert',
    '403' : 'Rose',
    '172' : 'Blanc-bleu',
    '171' : 'Violet',
    '77' : 'Rouge',
    '249' : 'Rouge bordeaux',
    '147' : 'Bleu azur',
    '94' : 'Noir',
    '300' : 'Purple',
    '235' : 'Turquoise'
}

SIZE_MAPPING = {
    '215' : 'M'
}

cdiscount_list = [
    "Seller product ref", "Barcode", "Brand", "Product nature",
    "Category code", "Short product label", "Long product label",
    "Product description", "Image 1" , "Family sku", "Size (limited)",
    "Marketing colour", "Product's marketing description", "Image 2",
    "Image 3", "Image 4", "MFPN", "Sous-état", "Licence", "Licences",
    "Type de Produit", "Gamme", "Description du produit", "Collection",
    "Modèles", "Composition du lot", "Lavable", "Pays d'origine",
    "Certifications et normes", "Plus produit", "Forme", "Confort",
    "Mentions légales", "Produit adapté à", "Type de fabrication",
    "Type de rideau", "Extensible", "Composition", "Type de fibre",
    "Type de tissu", "Garnissage", "Enveloppe", "Déhoussable",
    "Densité de tissage", "Grammage", "Finitions", "Fermeture",
    "Type de chaleur", "Couleur principale", "Couleur(s)", "Motif",
    "Motifs", "Style", "Utilisation", "Dimensions", "Dimensions du linge",
    "Dimensions bonnet", "Usage unique", "Type d'attaches", "Type de pièce",
    "Traitement de protection", "Conseil d'entretien",
    "Conseils d'utilisation",
    "Dimensions brutes - article emballé (L x l x H)", "Poids emballé",
    "Poids net", "Garantie (²)", "Garantie additionnelle", "Observations",
    "durée de disponibilité des pièces détachées essentielles à l’utilisation du produit",
    "Notes", "Labels et certifications"
]

ITEM_LIST = []
ITEM_ID_LIST =[]
IMAGE_LIST = []
TEXT_LIST = []
ERROR_LIST = []
ERROR_TEXT_LIST = []

def reverse(lst):
    return [ele for ele in reversed(lst)]

def connect():
    """
    Connect to the plentyAPI:

    Return:
                    [PlentyApi] -   Api used for data extraction
    """
    api = plenty_api.PlentyApi(
        base_url='https://panasiam.plentymarkets-cloud01.com',
        use_keyring=True,
        debug=True
    )
    return api

def extract_data(api):
    """
    Get all the variations from the API that have the referrerId of cdiscount.
    Then cycle through the json file for the data that is needed and do checks
    if they fulfill cdiscounts requirements and put them into a list of lists.

    Parameters:
        api         [PlentyApi] -   Api where data is to be extracted from
    """

    variations = api.plenty_api_get_variations(
        refine = {'referrerId':'143'}, additional = [
            'properties', 'variationBarcodes', 'marketItemNumbers',
            'variationCategories', 'variationDefaultCategory', 'images',
            'variationAttributeValues', 'variationSkus',
            'parent', 'item'
        ],
        lang='fr'
    )

    image_block = []
    img = False
    for variation in variations:
        err = False

        if variation['isMain'] == True:
            ITEM_ID_LIST.append(str(variation['itemId']))

        try:
            color_id = str(
                variation['variationAttributeValues'][0]['attributeValue']['id']
            )
            marketing_color = MARKETING_COLOR_MAPPING[color_id]
            if len(marketing_color) > 50:
                marketing_color = 'Too long'
                err = True
        except:
            marketing_color = 'Not Found'
            err = True

        if marketing_color == '':
            err = True
            marketing_color = 'Empty Value'

        try:
            for prop in variation['properties']:
                if prop['propertyId'] == 74:
                    size_id = str(prop['relationValues'][0]['value'])
                    size = SIZE_MAPPING[size_id]
        except:
            size = 'Not Found'
            err = True

        if size == '':
            err = True
            size = 'Empty Value'

        try:
            barcode = str(variation['variationBarcodes'][0]['code'])
            if not len(barcode) == 13:
                barcode = 'Not 13 chars long'
                err = True
        except IndexError:
            barcode = 'Not Found'
            err = True

        if barcode == '':
            err = True
            barcode = 'Empty Value'

        try:
            parent_sku = variation['variationSkus'][4]['parentSku']
            if len(parent_sku) > 50:
                parent_sku = 'Too long'
                err = True
        except IndexError:
            parent_sku = 'Not Found'
            err = True

        if parent_sku == '':
            err = True
            parent_sku = 'Empty Value'

        try:
            seller_ref = str(variation['marketItemNumbers'][0]['variationId'])
            if len(seller_ref) > 50:
                seller_ref = 'Too long'
                err = True
        except IndexError:
            seller_ref = 'Not Found'
            err = True

        if seller_ref == '':
            err = True
            seller_ref = 'Empty Value'

        brand = 'PANASIAM'
        try:
            branch_id = str(
                variation['variationDefaultCategory'][0]['branchId']
            )
            category_id = CATEGORY_ID_MAPPING[branch_id]
        except IndexError:
            category_id = 'Not Found'
            err = True

        if category_id == '':
            err = True
            category_id = 'Empty Value'

        product_nature = 'Standard'

        for image in variation['images']:
            for availability in image['availabilities']:
                if availability['value'] == 143:
                    img = True

            if img:
                image_block.append(image['url'])
                img = False

        if image_block == []:
            err = True
            image_block = ['No Image found']

        if err:
            ERROR_LIST.append([
                seller_ref, barcode, brand, product_nature, category_id,
                image_block[0], parent_sku, size, marketing_color,
                image_block[-1]
            ])
            err = False
            image_block = []
            continue

        ITEM_LIST.append([
            seller_ref, barcode, brand, product_nature, category_id,
            image_block[0], parent_sku, size, marketing_color,
            image_block[-1]
        ])
        IMAGE_LIST.append(image_block)
        image_block = []

def get_texts(api):
    """
    Get all the parents from the variations which have been extracted in
    extract_data(). Then cycle through the json file for the text data that
    is needed and do checks if they fulfill cdiscounts requirements and put
    them into a list of lists  and after put them into the item list created
    by extract_data().

    Parameters:
        api         [PlentyApi] -   Api where text data is to be extracted from
    """
    ITEM_STRING_LIST = "','".join(ITEM_ID_LIST)

    items = (api.plenty_api_get_items(
        refine={'id':ITEM_STRING_LIST}, lang='fr'
    ))

    for item in items:
        err = False
        parent_sku = str(item['id'])

        if len(item['texts'][0]['description']) <= 5000:
            long_description = item['texts'][0]['description']
        else:
            err = True
            long_description = 'Text too long'

        if len(item['texts'][0]['name1']) <= 30:
            short_label = item['texts'][0]['name1']
        else:
            err = True
            short_label = 'Text too long'

        if len(item['texts'][0]['name2']) <= 132:
            long_label = item['texts'][0]['name2']
        else:
            err = True
            long_label = 'Text too long'

        if len(item['texts'][0]['shortDescription']) <= 420:
            short_description = item['texts'][0]['shortDescription']
        else:
            err = True
            short_description = 'Text too long'

        if err:
            ERROR_TEXT_LIST.append([parent_sku, short_label, long_label,
                              short_description, long_description])
            err = False
        else:
            TEXT_LIST.append([parent_sku, short_label, long_label,
                             short_description, long_description])

    count_list = []
    for error in ERROR_TEXT_LIST:
        for count, item in enumerate(ITEM_LIST):
            if error[0] == item[6]:
                ERROR_LIST.append(item+error)
                count_list.append(count)
    count_list = reverse(count_list)

    for i in count_list:
        ITEM_LIST.pop(i)


    for text in TEXT_LIST:
        for item in ITEM_LIST:
            if text[0] == item[6]:
                item.insert(5, text[1])
                item.insert(6, text[2])
                item.insert(7, text[3])
                item.insert(12, text[4])

def write_xlsx():
    """
    Put all the extracted data into a dataframe and write into an excel file.
    """
    df = pd.DataFrame(ITEM_LIST)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Linge de maison - rideau - store'
    ws['A1'] = 'Model:'
    ws['B1'] = 'Linge de maison - rideau - store'
    ws.append(['',''])
    ws.append(cdiscount_list)
    ws.append(['',''])
    for row in dataframe_to_rows(df, index = False, header = False):
        ws.append(row)

    wb.save(filename = 'cdiscount_test1.xlsx')

def write_error():

    df = pd.DataFrame(ERROR_LIST)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Linge de maison - rideau - store'
    ws['A1'] = 'Model:'
    ws['B1'] = 'Linge de maison - rideau - store'
    ws.append(['',''])
    ws.append([
        'seller_ref', 'barcode', 'brand', 'product_nature',
        'category_id', 'image_1', 'parent_sku', 'size', 'marketing_color',
        'image_2', 'short_label', 'long_label', 'short_description',
        'long_description'
    ])
    ws.append(['',''])
    for row in dataframe_to_rows(df, index = False, header = False):
        ws.append(row)

    wb.save(filename = 'error.xlsx')

def main():
    API = connect()
    extract_data(API)
    get_texts(API)
    write_xlsx()
    write_error()
