-- Indexes for GroundTruth database

-- Selskap
create index if not exists selskap_orgnr_idx on selskap (orgNr);
-- Konkurs
create index if not exists konkurs_orgnr_idx on konkurs (OrgNr);
-- Aksjeeiebok
create index if not exists aksjeeiebok_orgnr_idx on aksjeeiebok (orgNr);
create index if not exists aksjeeiebok_selskap_idx on aksjeeiebok (selskap);
create index if not exists aksjeeiebok_aksjonærnavn_idx on aksjeeiebok (aksjonærNavn);
-- Eierskap
create index if not exists eierskap_utsteder_orgnr_idx on eierskap (utstederOrgNr);
create index if not exists eierskap_eierselskap_orgnr_idx on eierskap (eierSelskapOrgNr);
-- Person
create index if not exists person_navn_idx on person (navn);
create index if not exists person_selskap_orgnr_idx on person (selskapOrgNr);
create index if not exists person_selskap_rolle_idx on person (selskapRolle);
-- Politikere
create index if not exists politikere_navn_idx on politikere (navn);
create index if not exists politikere_parti_idx on politikere (parti);
create index if not exists politikere_kommuneNr_idx on politikere (kommuneNr);
