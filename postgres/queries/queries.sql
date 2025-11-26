-- konkurs_orgnr_dato
select navn, orgnr, oppløstDato
from konkurs

-- aksjeeiebok_equinor
select selskap, aksjonærNavn
from aksjeeiebok
where aksjonærNavn like '%EQUINOR%'

-- konkurs_varighet
select navn, oppløstdato-etablertdato as dager,
round((oppløstdato-etablertdato)/365.0,2) as år
from konkurs

-- kortest_levetid_selskap
select navn, organisasjonstype, oppløstdato-etablertdato as dager
from konkurs
where oppløstdato-etablertdato = (select min(dager)
                                  from (select oppløstdato-etablertdato as dager from konkurs))

-- politikere_fellesnavn
with 
p(navn,antall) as
(select navn, count(fødselsdato)
from politikere
group by navn
having count(*)>3)

select x.navn, x.fødselsdato, x.parti, x.kommune
from p, politikere x
where p.navn = x.navn
order by x.navn

-- allepersoner_storebokstaver
with 
allepersoner(navn, fødselsdato, kommune, tabell) as
(
(select upper(navn), fødselsdato, upper(kommune),'politikere' from politikere where fødselsdato is not null)
union
(select upper(navn), fødselsdato, upper(kommunenavn),'person' from person where fødselsdato is not null)
union
(select upper(eierpersonnavn), eierpersonfødselsdato,upper(eierpersonkommune),'eierskap'
 from eierskap where eierpersonfødselsdato is not null)
)
select navn, fødselsdato, kommune, tabell
from allepersoner
order by navn

-- gjenoppstaaende_selskaper
select s.navn, s.orgnr as aktiv, k.orgnr as konkurs, k.etablertdato, k.oppløstdato, s.etablertdato
from selskap as s, konkurs as k
where s.navn=k.navn

-- politikere_eierskap_konkurs
select eierPersonNavn, parti, konkurs.navn
from eierskap, konkurs, politikere
where eierPersonNavn is not null and
utstederUUID = konkurs.UUID and
politikere.navn=eierPersonNavn and
extract(year from politikere.fødselsdato)=eierPersonFødselsår

-- politikeres_roller_tidligere_konkurs
select po.navn as person, po.parti as parti, ko.navn as selskap, pe.selskaprolle as rolle, 
       extract(year from pe.selskaprolleregistrerttid) as registrert
from person as pe, politikere as po, konkurs as ko
where  po.navn=pe.navn and 
       pe.fødselsår =  extract(year from po.fødselsdato) and
       pe.selskapnavn=ko.navn
order by po.navn

-- innvalgt_politikere_selger_selskap
select e.eierPersonNavn, p.parti, k.navn, e.eierskapÅr , k.oppløstDato	
from eierskap e, konkurs k, politikere p
where e.eierPersonNavn is not null and
      k.uuid =utstederUUID and
      p.navn=e.eierPersonNavn and
      p.innvalgt='Ja'
and extract(year from k.oppløstDato)-e.eierskapÅr <=1

-- minst_konkurser_organisasjonstype
with mintype(antall) as 
(select min(antall) from
  (select organisasjonstype, count(*) as antall
   from konkurs
   group by organisasjonstype)
   )
(
(select organisasjonstype, count(*) as antall
   from konkurs
   group by organisasjonstype
   having count(*)=(select * from mintype))
)

-- roller_kautokeino_venstre
select navn, fødselsdato, selskaprolle, 
personselskaprollestartdato as start, personselskaprollesluttdato as slutt
from person
where selskapnavn='KAUTOKEINO VENSTRE'
order by personselskaprollestartdato

-- max_roller_medicinerforeningen
with maxrolle(antall) as
(select max(antall)
from (select navn, fødselsdato, count(selskaprolle) AS antall
      from person
      where selskapnavn='MEDICINERFORENINGEN'
      group by navn, fødselsdato)
      )
(
select navn, fødselsdato 
from person
where selskapnavn='MEDICINERFORENINGEN'
group by navn, fødselsdato, selskapuuid
having count(selskaprolle)=(select * from maxrolle)
)

-- antall_roller_politikere
select po.navn, po.fødselsdato, count(*) as antall
from politikere po, person pe
where po.navn=pe.navn and po.fødselsdato=pe.fødselsdato
group by po.navn, po.fødselsdato

-- politikere_rolle_naringsliv_antall
select count(*)
from (select po.navn as n, po.fødselsdato as f, count(*) as antall
from politikere po, person pe
where po.navn=pe.navn and po.fødselsdato=pe.fødselsdato
group by po.navn, po.fødselsdato)

-- politikere_roller_siste_aar
select po.navn, po.fødselsdato , personselskaprollestartdato,personselskaprollesluttdato
from politikere po, person pe
where po.navn=pe.navn and po.fødselsdato=pe.fødselsdato and
            ( personselskaprollesluttdato is null or 
        (select current_date)-personselskaprollesluttdato < 365 or
        (select current_date)-personselskaprollestartdato < 365)

-- rodt_roller_utenfor_kommune
select po.navn, po.fødselsdato, count(*) as antall
from politikere po, person pe
where po.navn=pe.navn and po.fødselsdato=pe.fødselsdato and 
      po.parti='Rødt' and upper(po.kommune)<>upper(pe.kommunenavn)
group by po.navn, po.fødselsdato
