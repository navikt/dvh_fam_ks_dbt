{{
    config(
        materialized='incremental'
    )
}}

with kontanststotte_meta_data as (
  select * from {{ref ('ks_meldinger_til_aa_pakke_ut')}}
),

pre_final as (
select * from kontanststotte_meta_data,
  json_table(melding, '$'
    COLUMNS (
      behandlings_id      path '$.behandlingsId',
      NESTED              PATH '$.kompetanseperioder[*]'
      COLUMNS (
         tom                            VARCHAR2 PATH '$.tom'
        ,fom                            VARCHAR2 PATH '$.fom'
        ,kompetanse_aktivitet           VARCHAR2 PATH  '$.kompetanseAktivitet'
        ,kompetanse_Resultat            VARCHAR2 PATH '$.resultat'
        ,barnets_bostedsland            Varchar2 path '$.barnetsBostedsland'
        ,SOKERS_AKTIVITETSLAND           Varchar2 path '$.sokersAktivitetsland'
        ,ANNEN_FORELDERS_AKTIVITET        Varchar2 path '$.annenForeldersAktivitet'
        ,ANNEN_FORELDERS_AKTIVITETSLAND   Varchar2 path '$.annenForeldersAktivitetsland'
        ,ANNEN_FORELDER_OMFATTET_AV_NORSK_LOVGIVNING VARCHAR2 path '$.annenForelderOmfattetAvNorskLovgivning'
    ))
    )j
    where json_value (melding, '$.kompetanseperioder.size()' )> 0
  ),

final as (
  select
  behandlings_id as fk_ks_fagsak,
  tom,
  fom,
  kompetanse_aktivitet,
  kompetanse_Resultat,
  barnets_bostedsland,
  kafka_offset,
  SOKERS_AKTIVITETSLAND,
  ANNEN_FORELDERS_AKTIVITET,
  ANNEN_FORELDERS_AKTIVITETSLAND,
  CASE when ANNEN_FORELDER_OMFATTET_AV_NORSK_LOVGIVNING = 'false' then 0
    else 1
  END ANNEN_FORELDER_OMFATTET_AV_NORSK_LOVGIVNING
  from pre_final
)

select
  dvh_fam_ks.hibernate_sequence.nextval as PK_KS_KOMPETANSE_PERIODER
  ,FOM
  ,TOM
  ,FK_KS_FAGSAK
  ,KOMPETANSE_AKTIVITET
  ,SOKERS_AKTIVITETSLAND
  ,ANNEN_FORELDERS_AKTIVITET
  ,ANNEN_FORELDERS_AKTIVITETSLAND
  ,BARNETS_BOSTEDSLAND
  ,kompetanse_Resultat
  ,ANNEN_FORELDER_OMFATTET_AV_NORSK_LOVGIVNING
  ,localtimestamp as LASTET_DATO
  ,kafka_offset
from final


