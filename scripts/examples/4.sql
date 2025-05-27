declare @statusReqSobran int,
    @req_guid varchar(40),
    @req_id int,
    @url varchar(4000),
    @type varchar(10);

select @req_guid = GUID,
       @req_id = A_OUID
from REQ_SERV rs
where rs.A_OUID = #id#;

select @statusReqSobran = A_OUID
from STATUS_PROC_REQ_SERV
where A_CODE = 3;

if exists (select req.A_OUID
           from REQ_SERV req
                    inner join LINK_REQ_SERV_RAY link
                               on link.A_FROMID = req.A_OUID
                    inner join REFERENCE_INF ref
                               on ref.A_OUID = link.A_TOID
           where req.A_OUID = @req_id
             and ref.A_IP_ADRESS_RAION not like '%ecn%'
          )
        select 'В районах запроса должен быть указан только ЕЦН' as error

if not exists (select 1 as mess
               from REQ_SERV as req
                        inner join LINK_REQ_SERV_RAY as link
                                   on link.A_FROMID = req.A_OUID
               where isnull(req.A_STATUS, 10) = 10
                 and req.A_OUID = @req_id
                 and link.A_STATUS_PROC != @statusReqSobran
              )
        select 'Сбор по данному объекту был выполнен ранее' as message
    else
        select @req_id as id

select @url = isnull(stuff((select distinct
                                   isnull(',{' + ri.A_IP_ADRESS_RAION + '}', '')
                            from REQ_SERV as req
                                     inner join LINK_REQ_SERV_RAY as link
                                                on link.A_FROMID = req.A_OUID
                                     inner join REFERENCE_INF ri
                                                on link.A_TOID = ri.A_OUID
                            where req.A_OUID = @req_id
                              and link.A_STATUS_PROC != @statusReqSobran
                              and isnull(ri.A_STATUS, 10) = 10
                            for xml path ('')
                           )
                         , 1, 1, ''), '{empty}')

select @req_guid as req_guid
     , @url      as url

if (@url = '{empty}')
        begin
            select 'Не указаны районы в запросе' as message
        end


/*Статистика по запросу сбора назначений*/
select @type = isnull(A_TYPE, 'test')
from G_CONFIG_SUB_SYSTEM_UN_INFOOBMEN
where A_IS_ACTIVE = 1

delete
from STATS_COLLECT_REQUEST
where A_STATUS = 70
  and A_REQ_SERV = @req_id;

delete stat
from STATS_COLLECT_REQUEST as stat
where stat.A_REQ_SERV = @req_id
  and stat.A_STATUS_REQUEST = 0

insert into STATS_COLLECT_REQUEST (GUID, A_CREATEDATE, A_CROWNER, A_TS, A_EDITOR, A_STATUS, A_REQ_SERV, RAYOUID,
                                   A_STATUS_REQUEST)
select newid()                        as GUID
     , getdate()                      as A_CREATEDATE
     , #login#                        as A_CROWNER
     , getdate()                      as A_TS
     , #login#                        as A_EDITOR
     , 10                             as A_STATUS
     , @req_id/*curRequest.A_FROMID*/ as A_REQ_SERV
     , rayon.A_OUID                 as RAYOUID
     , case
           when curRequest.A_TOID is null
               then 0
           else 1
    end
from REFERENCE_INF as rayon
         left join LINK_REQ_SERV_RAY as curRequest
                   on curRequest.A_TOID = rayon.A_OUID
                       and curRequest.A_FROMID = @req_id
where isnull(rayon.A_STATUS, 10) = 10
  and rayon.A_TYPE = @type
  and rayon.A_IP_ADRESS_RAION like '%ecn%'
  and isnull(rayon.A_STATUS, 10) = 10
  and not exists (select stats.A_OUID
                  from STATS_COLLECT_REQUEST as stats
                  where stats.RAYOUID = rayon.A_OUID
                    and stats.A_REQ_SERV = @req_id
                    and curRequest.A_STATUS_PROC != @statusReqSobran
                 )