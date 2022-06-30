with par as (
    select 19262731541000                                         as warehouse_id
         , to_timestamp('13.06.2022 00:00', 'dd.MM.yyyy HH24:mi') as p_bdate
         , to_timestamp('30.06.2022 00:00', 'dd.MM.yyyy HH24:mi') as p_edate
)
select pt.task_id                                          as 'Номер задания'
     , case
           when si.zone_id = 1633390 then '1.1 - 51 Мезонин 1 этаж подбор'
           when si.zone_id = 3622088 then '1.1 - 52 Мезонин 2 этаж подбор'
           when si.zone_id = 3624812 then '1.1 - 53 Мезонин 3 этаж подбор'
           when si.zone_id = 3625616 then '1.1 - 54 Мезонин 4 этаж подбор'
           when si.zone_id = 3657924 then '1.1 - 55 Мезонин 5 этаж подбор'
           when si.zone_id = 4984744 then '1.2 - 41 Мезонин 1 этаж подбор'
           when si.zone_id = 4593744 then '1.2 - 41/1 Стеллаж КГТ подбор'
           when si.zone_id = 1612616 then '1.2 - Паллетка Пикинг Нижний Ярус'
           when si.zone_id = 4986484 then '1.2 - 42 Мезонин 2 этаж подбор'
           when si.zone_id = 4987925 then '1.2 - 43 Мезонин 3 этаж подбор'
           when si.zone_id = 4987926 then '1.2 - 44 Мезонин 4 этаж подбор'
           when si.zone_id = 4987927 then '1.2 - 45 Мезонин 5 этаж подбор'
           else 'Сектор не выбран' end                     as 'Сектор подбора'
     , sm.name                                             as 'Метод сортировки '
     , pi.cnt_items                                        as 'кол-во sku'
     , pi.qty                                              as 'кол-во товаров'
     , pi.cnt_cells                                        as 'кол-во ячеек отбора'
     , tb.cnt_boxings                                      as 'кол-во тар'
     , s.cnt_skips                                         as 'кол-во обнулений'
     , to_char(tl20.start_at, 'dd.MM.yyyy HH24:mi:ss')     as 'время начала обработки задания'
     , to_char(pi2.first_pick_at, 'dd.MM.yyyy HH24:mi:ss') as 'время сканирования первого товара в заднии'
     , to_char(tl.at, 'dd.MM.yyyy HH24:mi')                as 'время окончания задания'
     , datediff('minute', tl20.start_at, tl.at)            as 'время подбора в минутах'
--  , pi3.volume
from wms_csharp_service_task.tasks_log tl
         join wms_csharp_service_task.tasks t on t.id = tl.task_id
         join par on par.warehouse_id = t.warehouse_id
         left join (
    select pt.task_id
         , pt.batch_id
         , pt.sector_id
    from wms_csharp_service_picking.tasks pt
    order by pt.task_id
) pt on pt.task_id = tl.task_id
         left join (
    select tl20.task_id
         , min(tl20.at) as start_at
    from wms_csharp_service_task.tasks_log tl20
    where tl20.status = 20
    group by tl20.task_id
    order by tl20.task_id
) tl20 on tl20.task_id = tl.task_id
         left join (
    select count(distinct tb.boxing_id) as cnt_boxings
         , tb.task_id
    from wms_csharp_service_picking.tasks_boxings tb
    group by tb.task_id
    order by tb.task_id
) tb on tb.task_id = tl.task_id
         left join (
    select count(s.id) as cnt_skips
         , s.task_id
    from wms_csharp_service_picking.skips s
    group by s.task_id
    order by s.task_id
) s on s.task_id = tl.task_id
         left join (
    select pi.task_id
         , count(distinct pi.item_id) as cnt_items
         , count(distinct pi.cell_id) as cnt_cells
         , sum(pi.qty)                as qty
    from (
             select pi.task_id
                  , pi.item_id
                  , pi.cell_id
                  , pi.quantity as qty
             from wms_csharp_service_picking.picked_items pi
             union all
             select pi.task_id
                  , pi.cell_id as item_id
                  , pi.cell_id
                  , 1          as qty
             from wms_csharp_service_picking.picked_instances pi
         ) pi
    group by pi.task_id
    order by pi.task_id
) pi on pi.task_id = tl.task_id
         left join (
    select pi.task_id
         , min(pi.at) as first_pick_at
    from (
             select pi.task_id
                  , pi.at
             from wms_csharp_service_picking.picked_items pi
             union all
             select pi.task_id
                  , pi.at
             from wms_csharp_service_picking.picked_instances pi
         ) pi
    group by pi.task_id
    order by pi.task_id
) pi2 on pi2.task_id = tl.task_id
         left join (
    select task_id,
           sum(qty * volume) as volume
    from (
             select pi.task_id
                  , pi.item_id
                  , sum(pi.qty) as qty
                  , volume      as volume
             from (
                      select pi.task_id
                           , pi.item_id
                           , pi.quantity as qty
                      from wms_csharp_service_picking.picked_items pi
                      union all
                      select pi.task_id
                           , i.sku_id as item_id
                           , 1        as qty
                      from wms_csharp_service_picking.picked_instances pi
                               join wms_csharp_service_item.instances i on i.id = pi.instance_id
                  ) pi
                      left join (
                 select i.sourcekey                             as item_id
                      , ((w.Width * h.Height * d.Depth) * 1000) as volume
                 from dwh_data.anc_item i
                          join dwh_data.atr_item_name n using (itemid)
                          join dwh_data.Atr_Item_Height h using (itemid)
                          join dwh_data.Atr_Item_Depth d using (itemid)
                          join dwh_data.Atr_Item_Width w using (itemid)) ii on ii.item_id = pi.item_id
             group by pi.task_id, pi.item_id, volume
             order by pi.task_id) pi
    group by task_id
) pi3 on pi3.task_id = tl.task_id
         left join wms_topology.sector_info si on si.id = pt.sector_id
         left join (
    select b.batch_id
         , b.sort_method_id
    from wms_batching.batch b
             join par on par.warehouse_id = b.warehouse_id
    where b.created_at > par.p_bdate - interval '7' day
    order by b.batch_id
) b on b.batch_id = pt.batch_id
         left join wms_crud_settings_ss.sort_method sm on sm.id = b.sort_method_id
         left join wms_service_employee."user" u on tl.user_id = u.id
where tl.status = 30
  and t.type = 3
  and tl.at between par.p_bdate and par.p_edate