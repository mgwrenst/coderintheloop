create table if not exists politikere (
    navn VARCHAR(60),
    parti TEXT,
    kommuneNr SMALLINT,
    kommune VARCHAR(60),
    fødselsdato DATE,
    listeplass SMALLINT,
    stemmetillegg VARCHAR(10),
    personstemmer INTEGER,
    slengere INTEGER,
    endeligRangering SMALLINT,
    innvalgt VARCHAR(10)
)