create table if not exists konkurs (
    nr INTEGER,
    navn TEXT,
    UUID VARCHAR(50),
    OrgNr INTEGER,
    konkursFlagg SMALLINT,
    likvidasjonFlagg SMALLINT,
    naceKode REAL,
    organisasjonstype TEXT,
    oppløstDato DATE,
    etablertDato DATE
)