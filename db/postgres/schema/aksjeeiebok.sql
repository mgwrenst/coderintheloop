CREATE TABLE IF NOT EXISTS aksjeeiebok (
    orgNr INTEGER,
    selskap TEXT,
    aksjeklasse VARCHAR(30),
    aksjonærNavn TEXT,
    aksjonærNr VARCHAR(11),
    poststed VARCHAR(60),
    landkode VARCHAR(10),
    antallAksjer BIGINT,
    antallAksjerSelskap BIGINT,
    år SMALLINT
);