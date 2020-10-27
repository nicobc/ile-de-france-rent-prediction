##################################################################
# IMPORTS
##################################################################
import numpy as np
import pandas as pd
from tqdm.notebook import tqdm
tqdm().pandas()

from geopy.geocoders import BANFrance
from geopy.exc import GeocoderTimedOut
geolocator = BANFrance()

import os

import requests
from bs4 import BeautifulSoup
import re
from unidecode import unidecode

from datetime import datetime as dt

##################################################################

def clean(data_folder='data',
          guy_hoquet_path,
          laforet_path,
          orpi_path):

    # Utility functions
    def print_shape(df):
        print(f'Number of rows    = {df.shape[0]:,}')
        print(f'Number of columns = {df.shape[1]}')

    def get_address_from_descr(row, search_pattern, split_pattern):
        descr = unidecode(row.descr)
        descr = re.sub(r'(sainte?)\-(\w+)', r'\1 \2', descr, flags=re.IGNORECASE)
        search = re.search(search_pattern, descr, flags=re.IGNORECASE)
        if search:
            city = row.city if not row.city.upper().startswith('PARIS') else 'PARIS'
            addr = search.group(1).strip().lower()
            addr = re.split(split_pattern, addr)[0]
            return f'{addr} {city} {dept_dict.get(row.dept, row.dept)} France'.lower()
        else:
            if row.city.upper().startswith('PARIS'):
                search = re.search(metro, row.descr, flags=re.IGNORECASE)
                if search:
                    city = row.city if not row.city.startswith('PARIS') else 'PARIS'
                    return f'metro {search.group(1)} {city} {dept_dict.get(row.dept, row.dept)} France'.lower()
                else:
                    return np.nan
            else:
                return np.nan

    def get_address_from_city_and_dept(row):
        if row.city.upper().startswith('PARIS'):
            arrn = re.sub(r'0(\d)', r'\1', row.city.split()[1])
            arrn = '1er arrondissement' if arrn == '1' else f'{arrn}e arrondissement'
            return f'paris {arrn} france'
        else:
            return f'{row.city} {dept_dict.get(row.dept, row.dept)} France'.lower()

    def get_coords(row):
        loc = geolocator.geocode(row.address)
        try:
            return loc.latitude, loc.longitude
        except GeocoderTimedOut:
            return np.nan
        except AttributeError:
            return

    ##################################################################
    # READ AND PARSE GUY HOQUET DATA
    ##################################################################
    df_guy_hoquet = pd.read_csv(guy_hoquet_path, sep='|')

    # parse property type into new column : type
    df_guy_hoquet.loc[df_guy_hoquet['prop_type'].str.contains('Appartement|Studio|Duplex', flags=re.IGNORECASE), 'type'] = 'Appartement'
    df_guy_hoquet.loc[df_guy_hoquet['prop_type'].str.contains('Maison', flags=re.IGNORECASE), 'type'] = 'Maison'
    df_guy_hoquet.loc[(df_guy_hoquet['type'].isna()) & (df_guy_hoquet['descr'].str.contains('appartement', flags=re.IGNORECASE)),
           'type'] = 'Appartement'
    df_guy_hoquet.loc[(df_guy_hoquet.type.isna()) & (df_guy_hoquet.descr.str.contains('maison', flags=re.IGNORECASE)),
           'type'] = 'Maison'
    df_guy_hoquet = df_guy_hoquet.loc[df_guy_hoquet.type.notna(), :]
    df_guy_hoquet['is_house'] = (df_guy_hoquet['type'] == 'Maison').astype(int)
    df_guy_hoquet = df_guy_hoquet.drop(['prop_type', 'type'], axis=1)

    # parse price
    df_guy_hoquet['price'] = df_guy_hoquet['price'].str.split('€').str[0].str.replace(' ', '').astype(int)

    # parse feats to extract surface, rooms & bedrooms
    df_guy_hoquet['feats'] = df_guy_hoquet['feats'].apply(lambda lst: eval(lst))
    df_guy_hoquet['surface'] = df_guy_hoquet['feats'].str[0].str.replace(' m²', '').astype(float)
    df_guy_hoquet['rooms'] = df_guy_hoquet['feats'].str[1].str.split().str[0].astype(int)
    df_guy_hoquet['bedrooms'] = df_guy_hoquet['feats'].str.join('|').str.extract(r'\|(\d+)\schambre\(s\)\|')[0].fillna(0)
    df_guy_hoquet = df_guy_hoquet.drop('feats', axis=1)

    # parse feats2 to extract furnished
    df_guy_hoquet['feats2'] = df_guy_hoquet['feats2'].apply(lambda lst: eval(lst))
    df_guy_hoquet['furnished'] = df_guy_hoquet['feats2'].str.join('|').str.extract(r'\|(Meublé \w+)\|')[0].str.split().str[1]
    df_guy_hoquet.loc[(df_guy_hoquet.furnished.isna()) & (df_guy_hoquet.descr.str.contains('(?<!non )meublé', flags=re.IGNORECASE)),
           'furnished'] = 'Oui'
    df_guy_hoquet.loc[(df_guy_hoquet.furnished.isna()) & (df_guy_hoquet.descr.str.contains('meublé', flags=re.IGNORECASE)),
           'furnished'] = 'Non'
    df_guy_hoquet = df_guy_hoquet.drop('feats2', axis=1)
    df_guy_hoquet = df_guy_hoquet.loc[df_guy_hoquet.furnished.notna(), :]
    df_guy_hoquet['furnished'] = (df_guy_hoquet['furnished'] == 'Oui').astype(int)

    # parse city to extract dept & postcode & city
    df_guy_hoquet['dept'] = df_guy_hoquet['city'].str.extract(r'(\d\d)\d\d\d').astype(int)
    df_guy_hoquet = df_guy_hoquet.loc[df_guy_hoquet.dept != 29, :]
    df_guy_hoquet['postcode'] = df_guy_hoquet['city'].str.extract(r'(\d+)')
    df_guy_hoquet['city'] = df_guy_hoquet['city'].str.replace(r'\d+', '').str.strip().str.upper()
    df_guy_hoquet.loc[df_guy_hoquet.city == 'PARIS', 'city'] = 'PARIS ' + df_guy_hoquet['postcode'].str[-2:]
    df_guy_hoquet.loc[df_guy_hoquet.city == '', 'city'] = 'ORSAY'
    df_guy_hoquet.loc[df_guy_hoquet.city.str.startswith('PARIS'), 'dept'] = df_guy_hoquet.postcode
    df_guy_hoquet = df_guy_hoquet.drop(['neighborhood', 'postcode'], axis=1)

    # drop duplicates
    df_guy_hoquet = df_guy_hoquet.drop_duplicates()

    print('Guy Hoquet DataFrame shape')
    print('==========================')
    print_shape(df_guy_hoquet)

    ##################################################################
    # READ AND PARSE LAFORET DATA
    ##################################################################
    df_laforet = pd.read_csv(laforet_path, sep='|').drop(['conso', 'emiss', 'ref'], axis=1)

    # parse price
    df_laforet['price'] = df_laforet.price.str.split('€').str[0].astype(int)

    # parse feats to extract surface, rooms & bedrooms
    df_laforet['surface'] = df_laforet.feats.str.extract(r'(\d+) m²').astype(int)
    df_laforet['rooms'] = df_laforet.feats.str.extract(r'(\d+) pièce').astype(int)
    df_laforet['bedrooms'] = df_laforet.feats.str.extract(r'(\d+) chbre').fillna(0)
    df_laforet = df_laforet.drop('feats', axis=1)

    # parse title to extract property type and city
    df_laforet['type'] = df_laforet.title.str.split().str[0]
    df_laforet['is_house'] = (df_laforet.type == 'Maison').astype(int)
    df_laforet['city'] = df_laforet.title.str.extract(r'(?:Appartement|Maison)\s(?:\w\d)?(.+)')[0] \
                                         .str.replace('près de ', '').str.strip().str.upper()
    df_laforet = df_laforet.drop(['title', 'type'], axis=1)

    # add arrondissement to paris dept
    mask = df_laforet.city.str.startswith('PARIS')
    df_laforet.loc[mask, 'dept'] = (
        df_laforet.loc[mask, 'dept'].astype(str) + '0' + df_laforet.loc[mask, 'city'].str[-2:]
    )
    df_laforet = df_laforet.loc[df_laforet.city != 'PARIS', :]
    df_laforet.loc[df_laforet.dept == '94013', 'dept'] = '75013'
    df_laforet.loc[df_laforet.dept == '92016', 'dept'] = '75016'
    df_laforet.dept = df_laforet.dept.astype(int)

    # parse furnitures into furnished binary features
    df_laforet['furnished'] = (df_laforet.furnitures == 'is_furnished').astype(int)
    df_laforet = df_laforet.drop('furnitures', axis=1)

    # drop duplicates
    df_laforet = df_laforet.drop_duplicates()

    print('Laforêt DataFrame shape')
    print('=======================')
    print_shape(df_laforet)

    ##################################################################
    # READ AND PARSE ORPI DATA
    ##################################################################
    df_orpi = pd.read_csv(orpi_path, sep='|').drop(['conso', 'emiss', 'ref'], axis=1)

    # add arrondissement to paris dept
    df_orpi['city'] = df_orpi.city.str.upper()
    df_orpi.loc[df_orpi.city.str.startswith('PARIS'), 'city'] = df_orpi.city.str.replace(r'PARIS (\d)(?!\d)',
                                                                                         r'PARIS 0\1')
    mask = df_orpi.city.str.startswith('PARIS')
    df_orpi.loc[mask, 'dept'] = (
        df_orpi.loc[mask, 'dept'].astype(str) + '0' + df_orpi.loc[mask, 'city'].str[-2:]
    )

    # parse is_house from prop_type
    df_orpi['is_house'] = df_orpi.prop_type.str.startswith('Maison').astype(int)
    df_orpi = df_orpi.drop('prop_type', axis=1)

    # parse rooms
    df_orpi['rooms'] = df_orpi.rooms.str.split('pièce').str[0].astype(int)

    # parse surface
    df_orpi['surface'] = df_orpi.surface.str.split().str[0].str.replace(',', '.').astype(float)

    # parse furnished & bedrooms from feats
    df_orpi['furnished'] = df_orpi.feats.str.contains('Meublé').astype(int)
    df_orpi['bedrooms'] = df_orpi.feats.str.extract(r'(\d+) chambres?')[0].fillna(0).astype(int)
    df_orpi = df_orpi.drop('feats', axis=1)

    # parse price
    df_orpi['price'] = df_orpi.price.str.split('€').str[0]
    df_orpi = df_orpi.loc[df_orpi.price != 'Loué', :]
    df_orpi['price'] = df_orpi.price.astype(int)

    # drop duplicates
    df_orpi = df_orpi.drop_duplicates()

    print('Orpi DataFrame shape')
    print('====================')
    print_shape(df_orpi)

    ##################################################################
    # CONCAT DATAFRAMES AND GEOCODE ADDRESSES
    ##################################################################
    # concatenate dataframes
    df = pd.concat([df_guy_hoquet, df_laforet, df_orpi])

    # replace "ST(E)" by "SAINT(E)" to harmonize city names
    df.city = df.city.str.replace(r'\bST(?!E)\b', 'SAINT')
    df.city = df.city.str.replace(r'\bSTE\b', 'SAINTE')

    print_shape(df)

    # get list of subway stations in Paris from wikipedia
    URL = 'https://fr.wikipedia.org/wiki/Liste_des_stations_du_m%C3%A9tro_de_Paris'
    soup = BeautifulSoup(requests.get(URL).content)
    metro = '|'.join([unidecode(el.select_one('td a').text).replace(' - ', '-').upper()
                      for el in soup.select_one('table.wikitable').select('tr')[1:]])
    metro = f'({metro})'.replace('COMMERCE|', '')

    # prepare variables to be used in geocoding functions
    dept_dict = {
        75: '',
        77: 'SEINE-ET-MARNE',
        78: 'YVELINES',
        91: 'ESSONNE',
        92: 'HAUTS-DE-SEINE',
        93: 'SEINE-SAINT-DENIS',
        94: 'VAL-DE-MARNE',
        95: 'VAL D\'OISE'
    }

    search_pattern = (r'((?:rue(?!(?: tres)? calme| commercante| pavillonnaire| pietonne| sans passage'
                      r'| tres re| a deux pas| au pied de)'
                      r'|place(?! de? parking| (?:de )?statio| dans | de moto| pour y| les dispo| perdue'
                      r'|et de l\'ecole| privative| a moins de)'
                      r'|avenue(?! bordee de maisons| principale)'
                      r'|boulevard(?! peripherique)|quai(?! de seine))\s+[a-z\s\']+)')

    split_pattern = (r' a (?:montreuil|vincennes|suresnes|asnieres sur seine|neuilly|noisy le sec|'
                     r'ablon sur seine|etampes)|'
                     r'(?:au)?(?: pied du)? metro| qui se | a$|'
                     r' a proximite| (?:un )?studio| proche| agreable| ideal(?:ement)?(?: coloc)?|'
                     r' deux pieces| au$| anime le quartier| et | et$|'
                     r'(?<!de) paris$| appartement| dans | location| au sein | situe |'
                     r' en plein| centre levallois|place a deux minutes de la gare  ligne  reseau  est '
                    )

    # get address from descr
    df['address'] = df.progress_apply(lambda row: get_address_from_descr(row, search_pattern, split_pattern), axis=1)

    # get address from city and dept if getting address from descr isn't possible
    df.loc[df.address.isna(), 'address'] = df.loc[df.address.isna()].progress_apply(get_address_from_city_and_dept,
                                                                                    axis=1)

    # geocode addresses
    df['coords'] = df.progress_apply(get_coords, axis=1)

    # parse coords
    df = pd.concat([
        df.drop('coords', axis=1).reset_index(drop=True),
        pd.DataFrame(df.coords.tolist(), columns=['lat', 'lon'])
    ], axis=1)

    ##################################################################
    # REMOVE WRONGLY GEODED DATA AND SAVE DATA ON DISK
    ##################################################################
    # drop wrongly geocoded datapoints
    n = df.loc[(df.lat >= 49.3) | (df.lat < 48) | (df.lon >= 3.8)].shape[0]
    print(f'At least {n} addresses ({n/df.shape[0]:.2%}) were wrongly geocoded and will be dropped.')
    df = df.loc[(df.lat < 49.3) & (df.lat > 48) & (df.lon < 3.8), :]

    # drop descr, address and city columns
    df = df.drop(['descr', 'address', 'city'], axis=1)

    # write df on disk
    df.to_csv(os.path.join(data_folder, f'locations_{dt.now().year}_{dt.now().month}_clean.csv'), sep='|', index=False)
