CREATE TABLE IF NOT EXISTS selskap (
    nr INTEGER,
    navn TEXT,
    UUID VARCHAR(50),
    orgNr INTEGER,
    konkursFlagg SMALLINT,
    likvidasjonFlagg SMALLINT,
    naceKode REAL,
    organisasjonstype TEXT,
    oppløstDato DATE,
    etablertDato DATE
);