### Coder in the loop
### Diverse kode for å opprette Ground Truth-basen i Postgres
### Richard Moe, 07.10.2025

#### Forutsetninger:
#### 1. Postgres-basen GroundTruth0 må være opprettet på forhånd
#### 2. aksjeeiebok-filene fra 2003 til 2024 er slått sammen til
####    1 fil -> 'aksjeeiebok.csv', og at denne er renset for
####    enkelstående "-tegn i radene
####



####
####  Laster inn data fra SamarbeidsDesken sine .csv-filer
####  

import  psycopg2

conn = psycopg2.connect("dbname=GroundTruth0 user=richardmoe")
cur = conn.cursor()


#######  TABELL  aksjeeiebok  ####################################################
########## oppretter tabell fra aksjeeiebok.csv
c='''
create table aksjeeiebok
(orgNr integer,
 selskap text,
 aksjeklasse varchar(30),
 aksjonærNavn text,
 aksjonærNr varchar(11),
 poststed varchar(60),
 landkode varchar(10),
 antallAksjer bigint,
 antallAksjerSelskap bigint,
 år smallint
 );
 '''

cur.execute(c)
conn.commit()

########### laster inn data i aksjeeiebok-tabellen fra csv-fil
filnavn='/Users/richardmoe/Desktop/GroundTruth0/aksjeeiebok/aksjeeiebok.csv'

c=f'''
copy aksjeeiebok
from '{filnavn}'
with (FORMAT CSV, HEADER, delimiter ';');
'''
print('**** utfører:',c)

cur.copy_expert(c,open(filnavn))
conn.commit()
###############################################################





####  TABELL politikere ######################################

### oppretter tabell politikere fra politikere.csv
c='''
create table politikere
(navn varchar(60),
 parti text,
 kommuneNr smallint,
 kommune varchar(60),
 fødselsdato date,
 listeplass smallint,
 stemmetillegg varchar(10),
 personstemmer integer,
 slengere integer,
 endeligRangering smallint,
 innvalgt varchar(10)
 );
 '''
cur.execute(c)
conn.commit()


###### laster inn data i politikere-tabellen
filnavn='/Users/richardmoe/Desktop/GroundTruth0/files/politikere.csv'
c=f'''
copy politikere
from '{filnavn}'
with (FORMAT CSV, HEADER, delimiter ';');
'''
cur.execute("set datestyle='SQL,DMY'")  ##datoer i filen er på formen DD/MM/ÅÅÅ
print('**** utfører:',c)
cur.copy_expert(c,open(filnavn))
conn.commit()
#############################################################




####  TABELL konkurs  (bankrupt) ######################################

##### oppretter tabell konkurs
c='''
create table konkurs
(nr integer,
navn text,
UUID varchar(50),
OrgNr integer,
konkursFlagg smallint,
likvidasjonFlagg smallint,
naceKode real,
organisasjonstype text,
oppløstDato date,
etablertDato date
);
 '''

cur.execute(c)
conn.commit()


######## laster inn data 
filnavn='/Users/richardmoe/Desktop/GroundTruth0/files/bankrupt_2020_120925.csv'

c=f'''
copy konkurs
from '{filnavn}'
with (FORMAT CSV, HEADER, delimiter ',');
'''
print('**** utfører:',c)
cur.copy_expert(c,open(filnavn))
conn.commit()
#############################################################

####### fjerner overflødige kolonner fra konkurs
a='''
alter table konkurs drop column nr;
'''
cur.execute(a)
conn.commit()






####  TABELL  selskap  (companies) ######################################

##### oppretter tabell selskap
c='''
create table selskap
(nr integer,
navn text,
UUID varchar(50),
orgNr integer,
konkursFlagg smallint,
likvidasjonFlagg smallint,
naceKode real,
organisasjonstype text,
oppløstDato date,
etablertDato date
);
 '''
#### constraint selskap_PK primary key (UUID)
cur.execute(c)
conn.commit()


########## laster inn data i selskap-tabellen
filnavn='/Users/richardmoe/Desktop/GroundTruth0/files/companies_active_companies_pop_100925_v3.csv'

c=f'''
copy selskap
from '{filnavn}'
with (FORMAT CSV, HEADER, delimiter ',');
'''
print('**** utfører:',c)
cur.copy_expert(c,open(filnavn))
conn.commit()
###############################################################


####### fjerner overflødige kolonner fra selskap
a='''
alter table selskap drop column nr;
'''
cur.execute(a)
conn.commit()

######### fjerne overlapp mellom selskap og konkurs
#########(det er 22 rader overlapp fra original-filene, alle har konkurs-dato 2025-09-10)
q='''delete from selskap
where UUID in (select UUID from konkurs)
'''
cur.execute(q)
conn.commit()



##########
########## oppretter view alleSelskap som slår sammen selskap og konkurs 
v='''
create view alleSelskaper as
select *
from selskap
union
select *
from konkurs
'''
cur.execute(v)
conn.commit()




####  TABELL  person (persons) ######################################

####### oppretter tabell person
c='''
create table person
(nr integer,
UUID varchar(50),
person_birth_day real,
navn text,
fødselsdato date,
fødselsår real,
person_birth_month real,
kjønnUUID varchar(50),
postnummer smallint,
person_street_name text,
land text,
postkode text,
person_street_letter text,
person_street_number text,
person_surrogate_key text,
person_surrugate_key smallint,
adresse text,
person_data_origin_ids text,
landKode varchar(10),
registrertTid timestamp,
oppdatertTid timestamp,
person_disambiguate_uuid varchar(50),
kommuneNr smallint,
kommuneNavn text,
person_person_master_uuid varchar(50),
person_composite_business_key text,
person_person_location_type_key text,
person_national_identification_number varchar(50),
person_national_identification_schema varchar(10),
selskapNavn text,
selskapUUID varchar(50),
selskapOrgNr bigint,
company_org_nr_schema varchar(10),
selskapRegistrertTid timestamp,
selskapOppdatertTid timestamp,
selskapRolleUUID varchar(50),
selskapRolle text,
selskapRolleRegistrertTid timestamp,
selskapRolleOppdatertTid timestamp,
selskapRolleRang text,
person_company_role_meta_role_elector_id text,
person_company_role_meta_role_responsibility text,
person_company_role_meta_role_responsibility_percentage real,
personSelskapRolleUUID varchar(50),
personSelskapRolleStartdato date,
personSelskapRolleSluttdato date,
personSelskapRollePersonUUID varchar(50),
person_company_role_business_key text,
personSelskapRolleSelskapUUID varchar(50),
person_company_role_external_url text,
person_company_role_resigned_flag text,
person_company_role_surrogate_key varchar(20),
person_company_role_surrugate_key text,
person_company_role_data_source_uuid varchar(50),
personSelskapRolleRegistrertTid timestamp,
personSelskapRolleOppdatertTid timestamp,
personSelskapRolleSelskapRolleUUID varchar(50)
);
 '''

cur.execute(c)
conn.commit()


########## laster inn data i persons-tabellen
filnavn='/Users/richardmoe/Desktop/GroundTruth0/files/persons_active_companies_pop_100925_v3_keep.csv'

c=f'''
copy person
from '{filnavn}'
with (FORMAT CSV, HEADER, delimiter ',');
'''
print('**** utfører:',c)
cur.copy_expert(c,open(filnavn))
conn.commit()
###############################################################



####### fjerner overflødige kolonner fra person
a='''
alter table person drop column nr;
alter table person drop column person_birth_day;
alter table person drop column person_birth_month;
alter table person drop column person_surrogate_key;
alter table person drop column person_surrugate_key;
alter table person drop column person_data_origin_ids;
alter table person drop column person_disambiguate_uuid;
alter table person drop column person_person_location_type_key;
alter table person drop column person_national_identification_number;
alter table person drop column person_national_identification_schema;
alter table person drop column person_company_role_meta_role_elector_id;
alter table person drop column person_company_role_meta_role_responsibility;
alter table person drop column person_company_role_meta_role_responsibility_percentage;
alter table person drop column person_company_role_external_url;
alter table person drop column person_company_role_resigned_flag;
alter table person drop column person_company_role_surrogate_key;
alter table person drop column person_company_role_surrugate_key;
alter table person drop column person_company_role_data_source_uuid;
alter table person drop column person_street_name;
alter table person drop column person_street_letter;
alter table person drop column person_street_number;
alter table person drop column person_person_master_uuid;
alter table person drop column person_composite_business_key;
alter table person drop column company_org_nr_schema;
alter table person drop column person_company_role_business_key;
alter table person drop column personSelskapRollePersonUUID;
alter table person drop column personSelskapRolleSelskapUUID;
alter table person drop column personSelskapRolleSelskapRolleUUID;
'''
cur.execute(a)
conn.commit()



####  TABELL  eierskap ######################################

####### oppretter tabell eierskap
c='''
create table eierskap
(nr integer,
eierPersonUUID varchar(50),
shareholder_person_birth_day real,
eierPersonNavn text,
eierPersonFødselsdato date,
eierPersonFødselsår real,
shareholder_person_birth_month real,
eierPersonKjønnUUID varchar(50),
eierPersonPostkode smallint,
eierPersonPoststed text,
eierPersonAdresse text,
eierPersonKommuneNr smallint,
eierPersonKommune text,
eierSelskapNavn text,
eierSelskapUUID varchar(50),
eierSelskapOrgNr integer,
utstederNavn text,
utstederUUID varchar(50),
utstederOrgNr integer,
eierskapUUID varchar(50),
eierskapÅr smallint,
eierskapAndel real,
eierskapAntall bigint,
company_share_ownership_ownership_lower real,
company_share_ownership_ownership_upper real,
eierskapAksjonær text,
eierskapStemmeandel real,
eierskapTotalAntall bigint,
eierskapStemmeantall bigint,
company_share_ownership_voting_ownership_lower real,
company_share_ownership_voting_ownership_upper real,
company_share_ownership_shareholder_person_uuid varchar(50),
company_share_ownership_shareholder_company_uuid varchar(50),
eierskapTotalStemmeantall bigint,
company_share_ownership_share_issuer_company_uuid varchar(50)
);
'''
cur.execute(c)
conn.commit()


############ laster inn data i eierskap-tabellen
filnavn='/Users/richardmoe/Desktop/GroundTruth0/files/ownerships_2023_2025.csv'

c=f'''
copy eierskap
from '{filnavn}'
with (FORMAT CSV, HEADER, delimiter ',');
'''
print('**** utfører:',c)
cur.copy_expert(c,open(filnavn))
conn.commit()
###############################################################


####### fjerner overflødige kolonner fra eierskap
a='''
alter table eierskap drop column nr;
alter table eierskap drop column shareholder_person_birth_month;
alter table eierskap drop column shareholder_person_birth_day;
alter table eierskap drop column company_share_ownership_ownership_lower;
alter table eierskap drop column company_share_ownership_ownership_upper;
alter table eierskap drop column company_share_ownership_voting_ownership_lower;
alter table eierskap drop column company_share_ownership_voting_ownership_upper;
alter table eierskap drop column company_share_ownership_share_issuer_company_uuid;
alter table eierskap drop column company_share_ownership_shareholder_person_uuid;
alter table eierskap drop column company_share_ownership_shareholder_company_uuid;

'''
cur.execute(a)
conn.commit()



###### testspørring
##q=\
##'''
##select År, selskap, navn_aksjonær
##from aksjeeiebok
##where selskap='VIKING ASSISTANCE GROUP AS'
##'''
##print(q)
##cur.execute(q)
##records = cur.fetchall()
##for x in records: print(x)

cur.close()


