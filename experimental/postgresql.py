import psycopg

# ------------------------
# Connection to database
# ------------------------

conninfo = "host=localhost dbname=groundtruth0 user=mgwrenst port=5432 password=password"

with psycopg.connect(conninfo) as conn:
    with conn.cursor() as cur:

        # ---------- TABLE aksjeeiebok ----------
        cur.execute("""
            create table if not exists aksjeeiebok
            (Orgnr integer,
             Selskap text,
             Aksjeklasse varchar(30),
             Navn_aksjonær text,
             Fødselsår_eller_orgnr varchar(11),
             Postnr_og_sted varchar(60),
             Landkode varchar(10),
             Antall_aksjer bigint,
             Antall_aksjer_selskap bigint,
             År smallint
            );
        """)
        cur.execute("TRUNCATE aksjeeiebok RESTART IDENTITY;")

        filename = "C:/Users/wren9/Downloads/GroundTruth0/aksjeeiebok/aksjeeiebok.csv"
        with open(filename, "r", encoding="utf-8") as f:
            with cur.copy("COPY aksjeeiebok FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ';')") as copy:
                for line in f:
                    copy.write(line)


        # ---------- TABLE politikere ----------
        cur.execute("""
            create table if not exists politikere
            (navn varchar(60),
             partinavn text,
             kommunenummer smallint,
             kommune varchar(60),
             fødselsdato date,
             listeplass smallint,
             stemmetillegg varchar(10),
             personstemmer integer,
             slengere integer,
             endelig_rangering smallint,
             innvalgt varchar(10)
            );
        """)
        cur.execute("SET datestyle = DMY;")
        cur.execute("TRUNCATE politikere RESTART IDENTITY;")

        filename = "C:/Users/wren9/Downloads/GroundTruth0/files/politikere.csv"
        with open(filename, "r", encoding="utf-8") as f:
            with cur.copy("COPY politikere FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ';')") as copy:
                copy.write(f.read())


        # ---------- TABLE bankrupt ----------
        cur.execute("""
            create table if not exists bankrupt
            (nr integer,
             company_name text,
             company_uuid varchar(50),
             company_org_nr integer,
             company_details_bankrupt_flag smallint,
             company_details_under_forced_liquidation_flag smallint,
             nace_code_primary_nace_code real,
             organization_type_organization_type_code text,
             company_establishment_dissolution_date date,
             company_establishment_establishment_date date
            );
        """)
        cur.execute("TRUNCATE bankrupt RESTART IDENTITY;")

        filename = "C:/Users/wren9/Downloads/GroundTruth0/files/bankrupt_2020_120925.csv"
        with open(filename, "r", encoding="utf-8") as f:
            with cur.copy("COPY bankrupt FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',')") as copy:
                copy.write(f.read())


        # ---------- TABLE companies ----------
        cur.execute("""
            create table if not exists companies
            (nr integer,
             company_name text,
             company_uuid varchar(50),
             company_org_nr integer,
             company_details_bankrupt_flag smallint,
             company_details_under_forced_liquidation_flag smallint,
             nace_code_primary_nace_code real,
             organization_type_organization_type_code text,
             company_establishment_dissolution_date date,
             company_establishment_establishment_date date
            );
        """)
        cur.execute("TRUNCATE companies RESTART IDENTITY;")

        filename = "C:/Users/wren9/Downloads/GroundTruth0/files/companies_active_companies_pop_100925_v3.csv"
        with open(filename, "r", encoding="utf-8") as f:
            with cur.copy("COPY companies FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',')") as copy:
                copy.write(f.read())

        # Remove overlap between bankrupt and companies
        cur.execute("""
            delete from companies
            where company_uuid in (select company_uuid from bankrupt);
        """)
        cur.execute("""
            create or replace view all_companies as
            select * from companies
            union
            select * from bankrupt;
        """)


        # ---------- TABLE persons ----------
        cur.execute("""
            create table if not exists persons
            (nr integer,
             person_uuid varchar(50),
             person_birth_day real,
             person_full_name text,
             person_birth_date date,
             person_birth_year real,
             person_birth_month real,
             person_gender_uuid varchar(50),
             person_postal_code smallint,
             person_street_name text,
             person_country_name text,
             person_postal_place text,
             person_street_letter text,
             person_street_number text,
             person_surrogate_key text,
             person_surrugate_key smallint,
             person_street_address text,
             person_data_origin_ids text,
             person_country_code_two varchar(10),
             person_insert_timestamp timestamp,
             person_update_timestamp timestamp,
             person_disambiguate_uuid varchar(50),
             person_municipality_code smallint,
             person_municipality_name text,
             person_person_master_uuid varchar(50),
             person_composite_business_key text,
             person_person_location_type_key text,
             person_national_identification_number varchar(50),
             person_national_identification_schema varchar(10),
             company_name text,
             company_uuid varchar(50),
             company_org_nr bigint,
             company_org_nr_schema varchar(10),
             company_insert_timestamp timestamp,
             company_update_timestamp timestamp,
             company_role_uuid varchar(50),
             company_role_company_role_key text,
             company_role_insert_timestamp timestamp,
             company_role_update_timestamp timestamp,
             company_role_company_role_rank text,
             person_company_role_meta_role_elector_id text,
             person_company_role_meta_role_responsibility text,
             person_company_role_meta_role_responsibility_percentage real,
             person_company_role_uuid varchar(50),
             person_company_role_to_date date,
             person_company_role_from_date date,
             person_company_role_person_uuid varchar(50),
             person_company_role_business_key text,
             person_company_role_company_uuid varchar(50),
             person_company_role_external_url text,
             person_company_role_resigned_flag text,
             person_company_role_surrogate_key varchar(20),
             person_company_role_surrugate_key text,
             person_company_role_data_source_uuid varchar(50),
             person_company_role_insert_timestamp timestamp,
             person_company_role_update_timestamp timestamp,
             person_company_role_company_role_uuid varchar(50)
            );
        """)
        cur.execute("TRUNCATE persons RESTART IDENTITY;")

        filename = "C:/Users/wren9/Downloads/GroundTruth0/files/persons_active_companies_pop_100925_v3_keep.csv"
        with open(filename, "r", encoding="utf-8") as f:
            with cur.copy("COPY persons FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',')") as copy:
                copy.write(f.read())

        cur.execute("""
            alter table persons
            drop column if exists person_birth_day,
            drop column if exists person_birth_year,
            drop column if exists person_birth_month,
            drop column if exists person_surrogate_key,
            drop column if exists person_surrugate_key,
            drop column if exists person_data_origin_ids,
            drop column if exists person_disambiguate_uuid,
            drop column if exists person_person_location_type_key,
            drop column if exists person_national_identification_number,
            drop column if exists person_national_identification_schema,
            drop column if exists person_company_role_meta_role_elector_id,
            drop column if exists person_company_role_meta_role_responsibility,
            drop column if exists person_company_role_meta_role_responsibility_percentage,
            drop column if exists person_company_role_external_url,
            drop column if exists person_company_role_resigned_flag,
            drop column if exists person_company_role_surrogate_key,
            drop column if exists person_company_role_surrugate_key,
            drop column if exists person_company_role_data_source_uuid,
            drop column if exists person_street_name,
            drop column if exists person_street_letter,
            drop column if exists person_street_number;        
        """)


        # ---------- TABLE eierskap ----------
        cur.execute("""
            create table if not exists eierskap
            (nr integer,
             shareholder_person_uuid varchar(50),
             shareholder_person_birth_day real,
             shareholder_person_full_name text,
             shareholder_person_birth_date date,
             shareholder_person_birth_year real,
             shareholder_person_birth_month real,
             shareholder_person_gender_uuid varchar(50),
             shareholder_person_postal_code smallint,
             shareholder_person_postal_place text,
             shareholder_person_street_address text,
             shareholder_person_municipality_code smallint,
             shareholder_person_municipality_name text,
             shareholder_company_name text,
             shareholder_company_uuid varchar(50),
             shareholder_company_org_nr integer,
             share_issuer_company_name text,
             share_issuer_company_uuid varchar(50),
             share_issuer_company_org_nr integer,
             company_share_ownership_uuid varchar(50),
             company_share_ownership_year smallint,
             company_share_ownership_ownership real,
             company_share_ownership_share_count bigint,
             company_share_ownership_ownership_lower real,
             company_share_ownership_ownership_upper real,
             company_share_ownership_shareholder_name text,
             company_share_ownership_voting_ownership real,
             company_share_ownership_total_share_count bigint,
             company_share_ownership_voting_share_count bigint,
             company_share_ownership_voting_ownership_lower real,
             company_share_ownership_voting_ownership_upper real,
             company_share_ownership_shareholder_person_uuid varchar(50),
             company_share_ownership_shareholder_company_uuid varchar(50),
             company_share_ownership_total_voting_share_count bigint,
             company_share_ownership_share_issuer_company_uuid varchar(50)
            );
        """)
        cur.execute("TRUNCATE eierskap RESTART IDENTITY;")

        filename = "C:/Users/wren9/Downloads/GroundTruth0/files/ownerships_2023_2025.csv"
        with open(filename, "r", encoding="utf-8") as f:
            with cur.copy("COPY eierskap FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',')") as copy:
                copy.write(f.read())

        cur.execute("""
            alter table eierskap
            drop column if exists shareholder_person_birth_year,
            drop column if exists shareholder_person_birth_month,
            drop column if exists shareholder_person_birth_day,
            drop column if exists company_share_ownership_ownership_lower,
            drop column if exists company_share_ownership_ownership_upper,
            drop column if exists company_share_ownership_voting_ownership_lower,
            drop column if exists company_share_ownership_voting_ownership_upper,
            drop column if exists company_share_ownership_share_issuer_company_uuid,
            drop column if exists company_share_ownership_shareholder_person_uuid,
            drop column if exists company_share_ownership_shareholder_company_uuid; 
        """)
