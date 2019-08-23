# -*- coding: utf-8 -*-
__author__ = '5446'
#
# Python 3.6
# Conda environment: python3
#
# BASE
#
# - postgres (psycopg)
# - Ubuntu postges db open
# - Farmplanner app-data open
# - Feeder data not open
#

import sys
import csv
import psycopg2
import psycopg2.extras
import sqlite3 as lite
from datetime import datetime, date, timedelta
import platform
print("The Python version is %s.%s.%s" % sys.version_info[:3])
os = platform.processor()
print(os)
if os == 'Intel64 Family 6 Model 42 Stepping 7, GenuineIntel':
    print('suoritetaan tarhan PC-koneella')
    tietokanta = 'UBUNTU'
else:
    if os == 'Intel64 Family 6 Model 94 Stepping 3, GenuineIntel':
        print('suoritetaan kotikoneella')
        tietokanta = 'PC'
    else:
        if os == 'x86_64':
            print('Suoritetaan koneella ACER UBUNTU')
            tietokanta = 'UBUNTU-UBUNTU'
        else:
            print('ollaan ilmeisesti jollain tarhan koneella')
            print('tiedostopolut määrittelemättä tälle koneelle')
            sys.exit()

print("pc")
#
# 'UBUNTU'
# 'PC"
# 'LOCALHOST'
#
# tietokanta = 'UBUNTU'
#
tietokanta_host = ''
if tietokanta == 'UBUNTU':
    tietokanta_host = '192.168.1.150'  # ubuntu nahoittamossa
if tietokanta == 'UBUNTU-UBUNTU':
    tietokanta_host = '192.168.1.150'  # ubuntu nahoittamossa
if tietokanta == 'PC':
    tietokanta_host = '192.168.1.103' # pöytä-pc kotona
if tietokanta == 'LOCALHOST':
    tietokanta_host = 'localhost'     # pöytä-pc:llä käytettynä
#
#-------- akk ---------- postgres connect------------
con_akk = None
con_akk = psycopg2.connect(host=tietokanta_host, dbname='akk_2017', user='postgres', password='abc')
if con_akk is not None:
    cur_akk = con_akk.cursor(cursor_factory=psycopg2.extras.DictCursor)
    print("AKK Postgres database on {} {} connected".format(tietokanta, tietokanta_host))
else:
    print("AKK Postgres database on {} {} not found".format(tietokanta, tietokanta_host))
#---------------------------------------------------------------
# --------------------------------------------------------------
# -------- FarmPlanner app -------------------------------------
# --------------------------------------------------------------
# --------------------------------------------------------------

con_lite_app = None

con_lite_feeder = None

if tietokanta == 'UBUNTU':
    con_lite_app = lite.connect(r'\\UBUNTU\Anonymous\fp-app-data-ubuntu\2019-08-03-fp-app.db')
    con_lite_feeder = lite.connect(r'D:/AKK/2017/FP-FEEDER-DATA/fpfeeder_20170916.sql3')
if tietokanta == 'UBUNTU-UBUNTU':
    con_lite_app = lite.connect(r'\\UBUNTU\Anonymous\fp-app-data-ubuntu\2017-08-30-fp-app.db')
    # con_lite_feeder = lite.connect(r'D:/AKK/2017/FP-FEEDER-DATA/fpfeeder_20170916.sql3')
if tietokanta == 'PC':
    con_lite_app = lite.connect(r'D:/AKK/2018/DB-APP/2018-03-03-fp-app.db')
    con_lite_feeder = lite.connect(r'D:/AKK/2017/FP-FEEDER-DATA/fpfeeder_20170916.sql3')
if tietokanta == 'LOCALHOST':
    con_lite_app = lite.connect(r'D:/AKK/2017/FP-APP-DATA/2017-08-29-fp-app.db')


print(con_lite_app)
# con_lite_app = lite.connect(r'//UBUNTU/Anonymous/fp-app-data_20161204084801.db')
# con_lite_app = lite.connect(r'D:/AKK/2016/FP_DATA/2016-07-11/2016-07-11/2016-07-11-app.lite')
con_lite_app.row_factory = lite.Row
if con_lite_app is not None:
    cur_app = con_lite_app.cursor()
    print('')
    print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
    print('App datafile connected')
    print(cur_app)
    print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
else:
    print('App datafile not found')

print(con_lite_feeder)
# con_lite_app = lite.connect(r'//UBUNTU/Anonymous/fp-app-data_20161204084801.db')
# con_lite_app = lite.connect(r'D:/AKK/2016/FP_DATA/2016-07-11/2016-07-11/2016-07-11-app.lite')
# con_lite_feeder.row_factory = lite.Row
if con_lite_feeder is not None:
    con_lite_feeder.row_factory = lite.Row
    cur_feeder = con_lite_feeder.cursor()
    print('')
    print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
    print('Feeder datafile connected')
    print(cur_feeder)
    print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
else:
    print('Feeder datafile not found')
#
# ==============================================================================
# ==============================================================================
# ==============================================================================
# ==============================================================================
table_name = 'animal'

SQL_columns = 'SELECT * FROM {}'.format(table_name)
cur_akk.execute(SQL_columns)
columns = [cn[0] for cn in cur_akk.description]
print('columns in {}'.format(table_name))
print('')
for column in columns:
    # print(column, end=' ')
    print('{}.{},'.format(table_name, column))

# ==============================================================================
nyt_paiva = date.today()
nyt_vuosi = nyt_paiva.year

# viikonpaivat = [    ['Maanantai'],
#                     ['Tiistai'],
#                     ['Keskiviikko'],
#                     ['Torstai'],
#                     ['Perjantai'],
#                     ['Lauantai'],
#                     ['Sunnuntai'] ]

viikonpaivat = [    ['Ma'],
                    ['Ti'],
                    ['Ke'],
                    ['To'],
                    ['Pe'],
                    ['La'],
                    ['Su'] ]

# mitkä talot
talot = [21, 22, 23, 24, 25, 26, 27, 28, 30, 36, 37, 38, 39, 18, 17, 16, 15, 14, 10, 8, 7, 4, 3, 2, 1]

# hae talo kerrallaan farmplannerin paikka id:t

for talo in talot:
    SQL_talon_fp_paikat = '''SELECT place_id_fp FROM place 
                             WHERE place.place_house = %s '''
    cur_akk.execute(SQL_talon_fp_paikat, (talo,))
    res_fp_paikat_talossa = cur_akk.fetchall()

    # tämän talon rehu per eläin eilen

    eilen_pvm = nyt_paiva - timedelta(days=1)

    # kuinka monta eläintä

    SQL_laske_rehurivit = '''SELECT COUNT(*) FROM feed
                             WHERE info_date  = %s and 
                                   place_id_fp = ANY(%s) '''

    cur_akk.execute(SQL_laske_rehurivit, (eilen_pvm, res_fp_paikat_talossa))
    res_kpl = cur_akk.fetchone()[0]

    # kuinka monta grammaa kaikki on ruokittu

    SQL_laske_rehu_ruokittu_g = '''SELECT sum(feed_actual) FROM feed
                                 WHERE info_date = %s and 
                                       place_id_fp = ANY(%s) '''

    cur_akk.execute(SQL_laske_rehu_ruokittu_g, (eilen_pvm, res_fp_paikat_talossa))
    res_rehu_ruokittu_g = cur_akk.fetchone()[0]
    ruokittu_g_elain = int(res_rehu_ruokittu_g / res_kpl)

    # kuinka monta grammaa resepti

    SQL_laske_rehu_resepti_g = '''SELECT sum(feed_recipe) FROM feed
                                     WHERE info_date = %s and 
                                           place_id_fp = ANY(%s) '''

    cur_akk.execute(SQL_laske_rehu_resepti_g, (eilen_pvm, res_fp_paikat_talossa))
    res_rehu_resepti_g = cur_akk.fetchone()[0]
    resepti_g_elain = int(res_rehu_resepti_g / res_kpl)

    # kuinka monta grammaa resepti + muutokset

    SQL_laske_rehu_muutoksilla_g = '''SELECT sum(feed_adjusted) FROM feed
                                         WHERE info_date = %s and 
                                               place_id_fp = ANY(%s) '''

    cur_akk.execute(SQL_laske_rehu_muutoksilla_g, (eilen_pvm, res_fp_paikat_talossa))
    res_rehu_muutoksilla_g = cur_akk.fetchone()[0]
    suunniteltu_g_elain = int(res_rehu_muutoksilla_g / res_kpl)

    toteutunut_prosentti_reseptista = int((ruokittu_g_elain / resepti_g_elain) * 100)
    toteutunut_prosentti_suunnitellusta = int((ruokittu_g_elain / suunniteltu_g_elain) * 100)

    print('talo {:2} eläimiä: {:3} resepti {:4} suunniteltu {:4} ruokittu {:4}  ----- reseptistä {:3} % suunnitellusta {:3} %'.format(
        talo,
        res_kpl,
        resepti_g_elain,
        suunniteltu_g_elain,
        ruokittu_g_elain,
        toteutunut_prosentti_reseptista,
        toteutunut_prosentti_suunnitellusta))

# ===========================================================================
#
# kaikki talot yhteensä
#

print('////////////////////////////////////////////////////////////////////////////////////')
print('kaikki talot - kaikki päivät')

# kaikki päivät joista on tiedot tänä vuonna

SQL_hae_paivat = '''SELECT DISTINCT info_date FROM feed 
                    WHERE extract(year from info_date) = %s 
                    ORDER BY info_date'''
cur_akk.execute(SQL_hae_paivat, (nyt_vuosi,))
res_paivat = cur_akk.fetchall()

SQL_talon_fp_paikat = '''SELECT place_id_fp FROM place 
                             WHERE place.place_house = ANY(%s) '''
cur_akk.execute(SQL_talon_fp_paikat, (talot,))
res_fp_paikat_talossa = cur_akk.fetchall()

# kaikkien talojen rehu per eläin pävittäin

for paiva in res_paivat:
    
    # kuinka monta eläintä
    
    SQL_laske_rehurivit = '''SELECT COUNT(*) FROM feed
                             WHERE info_date  = %s and 
                                   place_id_fp = ANY(%s) '''
    
    cur_akk.execute(SQL_laske_rehurivit, (paiva[0], res_fp_paikat_talossa))
    res_kpl = cur_akk.fetchone()[0]
    
    # kuinka monta grammaa kaikki on ruokittu
    
    SQL_laske_rehu_ruokittu_g = '''SELECT sum(feed_actual) FROM feed
                                 WHERE info_date = %s and 
                                       place_id_fp = ANY(%s) '''
    
    cur_akk.execute(SQL_laske_rehu_ruokittu_g, (paiva[0], res_fp_paikat_talossa))
    res_rehu_ruokittu_g = cur_akk.fetchone()[0]
    ruokittu_g_elain = int(res_rehu_ruokittu_g / res_kpl)
    
    # kuinka monta grammaa resepti
    
    SQL_laske_rehu_resepti_g = '''SELECT sum(feed_recipe) FROM feed
                                     WHERE info_date = %s and 
                                           place_id_fp = ANY(%s) '''
    
    cur_akk.execute(SQL_laske_rehu_resepti_g, (paiva[0], res_fp_paikat_talossa))
    res_rehu_resepti_g = cur_akk.fetchone()[0]
    resepti_g_elain = int(res_rehu_resepti_g / res_kpl)
    
    # kuinka monta grammaa resepti + muutokset
    
    SQL_laske_rehu_muutoksilla_g = '''SELECT sum(feed_adjusted) FROM feed
                                         WHERE info_date = %s and 
                                               place_id_fp = ANY(%s) '''
    
    cur_akk.execute(SQL_laske_rehu_muutoksilla_g, (paiva[0], res_fp_paikat_talossa))
    res_rehu_muutoksilla_g = cur_akk.fetchone()[0]
    suunniteltu_g_elain = int(res_rehu_muutoksilla_g / res_kpl)
    
    toteutunut_prosentti_reseptista = int((ruokittu_g_elain / resepti_g_elain) * 100)
    toteutunut_prosentti_suunnitellusta = int((ruokittu_g_elain / suunniteltu_g_elain) * 100)

    viikonpaiva = paiva[0].weekday()
    viikonpaiva_s = viikonpaivat[viikonpaiva]

    print('{} {} eläimiä: {:3} resepti {:4} suunniteltu {:4} ruokittu {:4}  ----- reseptistä {:3} % suunnitellusta {:3} %'.format(
        paiva[0],
        viikonpaiva_s[0],
        res_kpl,
        resepti_g_elain,
        suunniteltu_g_elain,
        ruokittu_g_elain,
        toteutunut_prosentti_reseptista,
        toteutunut_prosentti_suunnitellusta))

    if viikonpaiva_s[0] == 'Su':
        print('')

# ---- COMMIT -------
# con_akk.commit()
# con_akk.close()
print('')
print('xxxxxxxxx end xxxxxxxxxx')

