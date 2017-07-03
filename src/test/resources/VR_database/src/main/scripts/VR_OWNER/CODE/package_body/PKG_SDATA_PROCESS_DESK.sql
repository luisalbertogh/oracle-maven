--liquibase formatted sql
--changeset ${vr_owner_user}_CODE:PACKAGE_BODY_PKG_SDATA_PROCESS_DESK runOnChange:TRUE  failOnError:TRUE splitStatements:FALSE stripComments:FALSE


CREATE OR REPLACE PACKAGE BODY PKG_SDATA_PROCESS_DESK
AS
PROCEDURE P_PROCESS_DESKATTR
IS
    v_cnt_new_desk   number default 0;
    v_cnt_missing_desk number default 0;
    v_last_elapsed_month number(6);
    v_current_month_id number(6);
    v_cnt_legal_entity number default 0;
    v_vtd_start_date date;
    v_nvtd_start_date date;
    v_desk_l5ubr varchar2(200);
    v_desk_rpl_code varchar2(200);
    v_desk_source varchar2(200);
    v_enable_vri_upd varchar2(10);
	v_enable_rfs varchar2(10);
BEGIN

   dbms_output.put_line('Starting static data desk process for run date : '||to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
   v_current_month_id := to_char(sysdate, 'YYYYMM');
   v_vtd_start_date := add_months(trunc(sysdate, 'MONTH'), 1);
   v_nvtd_start_date := trunc(sysdate, 'MONTH');
   select decode1 into v_enable_vri_upd from sdata_decode_list where list_name='SDATA_VRI_ENABLE_UPD';
   select decode1 into v_enable_rfs from sdata_decode_list where list_name='SDATA_RFS_ENABLE_UPD';
   dbms_output.put_line('Current month end : '||v_current_month_id);
   dbms_output.put_line('VTD Start Date : '||v_vtd_start_date);
   dbms_output.put_line('Non-VTD Start Date : '||v_nvtd_start_date);
   dbms_output.put_line('VRI table update : '||v_enable_vri_upd);
   dbms_output.put_line('Enable RFS update: '||nvl(v_enable_rfs,'Disable'));
  -------------------------------------- VTD/NVTD Desk process  ---------------------------------
    -- check if any new desks which are exists in brds_vw_vtd, bh_non_vtd and bh_manual_hierarchy_elems tables
    -- for non-vtd we are looking only for current month and start date will have current month and for VTD; sdata_desk will have start_date as first of next month
    select count(*) into v_cnt_new_desk
    from (
        select volckertradingdesk as desk_rpl_code, 'bRDS-VTD' as desk_source from brds_vw_vtd union
        select non_vtd_rpl_code as desk_rpl_code, 'Non-VTD' as desk_source from bh_non_vtd where trunc(asofdate, 'MONTH') = v_nvtd_start_date union
        select rplcode as desk_rpl_code,'Manual_Elems' as desk_source from bh_manual_hierarchy_elems
    ) newd
    where not exists (select 1 from sdata_desk desk where desk.desk_rpl_code = newd.desk_rpl_code and desk.desk_source = newd.desk_source)
    and newd.desk_rpl_code not in ('00_EXCLUDE','00_NO_MATCH','None');
    dbms_output.put_line('Count of new desks :'||v_cnt_new_desk||' : '||sysdate);

    -- add new desks to desk dimension table with start date as first of next month
    insert into sdata_desk (desk_id, desk_rpl_code, desk_full_name, status, start_date, desk_source, update_user, update_datetime)
    select desk_id, desk_rpl_code, desk_full_name, 'Active' as status, start_date, desk_source, 'batch' as update_user, sysdate as update_datetime from (
    select volckertradingdeskid as desk_id, volckertradingdesk as desk_rpl_code, volckertradingdeskfull as desk_full_name, 'bRDS-VTD' as desk_source, v_vtd_start_date as start_date
    from brds_vw_vtd
    union
    select non_vtd_code as desk_id, non_vtd_rpl_code as desk_rpl_code, non_vtd_rpl_name as desk_full_name, 'Non-VTD' as desk_source, v_nvtd_start_date as start_date
    from bh_non_vtd where trunc(asofdate, 'MONTH') = v_nvtd_start_date
    union
    select 'MAN-ENTRY' as desk_id, rplcode as desk_rpl_code, name as desk_full_name, 'Manual_Elems' as desk_source, v_vtd_start_date as start_date
    from bh_manual_hierarchy_elems
    ) newd where not exists (select 1 from sdata_desk desk where desk.desk_rpl_code = newd.desk_rpl_code and desk.desk_source = newd.desk_source)
    and newd.desk_rpl_code not in ('00_EXCLUDE','00_NO_MATCH','None');
   dbms_output.put_line('Added new desks - rows : '||sql%rowcount);

   -- update desk name with new name from bRDS/Manual Entry/Non-VTD
   merge into sdata_desk desk
    using (
        select desk_rpl_code, desk_full_name, desk_source from (
        select volckertradingdesk as desk_rpl_code, volckertradingdeskfull as desk_full_name, 'bRDS-VTD' as desk_source
        from brds_vw_vtd
        union
        select non_vtd_rpl_code as desk_rpl_code, non_vtd_rpl_name as desk_full_name, 'Non-VTD' as desk_source
        from bh_non_vtd where trunc(asofdate, 'MONTH') = v_nvtd_start_date
        union
        select rplcode as desk_rpl_code, name as desk_full_name, 'Manual_Elems' as desk_source
        from bh_manual_hierarchy_elems
        ) newd where newd.desk_rpl_code not in ('00_EXCLUDE','00_NO_MATCH','None') and desk_full_name is not null
    ) newdesk
    on (desk.desk_rpl_code = newdesk.desk_rpl_code and desk.desk_source = newdesk.desk_source)
    when matched then
        update set desk.desk_full_name =  newdesk.desk_full_name, update_user='batch', update_datetime=sysdate
        where desk.desk_full_name <>  newdesk.desk_full_name ;

   dbms_output.put_line('Updated desk full name to latest- rows : '||sql%rowcount);

  /*
  --  commenting out as this will be handled in new code added next to this block
    -- initialize attributes for new desk
	insert into sdata_desk_attributes (desk_rpl_code, volcker_relevant, desk_descr, metrics_reportable, exemp_excl_type, control_type, pvf,
                                                                  business, division, start_date, end_date, desk_source, update_user, update_datetime, status)
	select  a.desk_rpl_code,  'N' as volcker_relevant, a.desk_descr, 'N' as metrics_reportable, exemp_excl_type, control_type, pvf, business, division, start_date, end_date
			  , 'Non-VTD' as desk_source
			  ,'init' as update_user, sysdate as update_datetime, 'Active' as status
	 from
	(
	select trim(non_vtd_rpl_code) as desk_rpl_code, null as desk_descr, case when NON_VTD_EXCLUSION_TYPE='EP'  then 'EP' else 'NT' end as exemp_excl_type,
			  case when NON_VTD_EXCLUSION_TYPE='EP'  then 'EP' else NON_VTD_EXCLUSION_TYPE end as control_type, non_vtd_pvf as pvf,
			  non_vtd_business as business, non_vtd_division as division,  v_nvtd_start_date as start_date, cast(null as date) as end_date
	from bh_non_vtd
	where trim(non_vtd_rpl_code)  not in (select desk_rpl_code from sdata_desk_attributes where desk_source='Non-VTD')
    and    trunc(asofdate, 'MONTH') = v_nvtd_start_date
	and    non_vtd_rpl_code <> 'None'
	order by 1
	) a;

	dbms_output.put_line('Added new desks attributes for Non-VTD - rows : '||sql%rowcount);
    */

    -- merge into attributes table for closing previous period if there are any changes to desk attributes
    merge into sdata_desk_attributes datt
    using (    select distinct desk_rpl_code, desk_source, start_date from (
                select  a.desk_rpl_code, exemp_excl_type, control_type, pvf, business, division, start_date
              , desk_source ,'batch' as update_user, sysdate as update_datetime, 'Active' as status from
                (
                select trim(non_vtd_rpl_code) as desk_rpl_code, case when NON_VTD_EXCLUSION_TYPE='EP'  then 'EP' else 'NT' end as exemp_excl_type,
                          case when NON_VTD_EXCLUSION_TYPE='EP'  then 'EP' else NON_VTD_EXCLUSION_TYPE end as control_type, non_vtd_pvf as pvf,
                          non_vtd_business as business, non_vtd_division as division,  v_nvtd_start_date as start_date, 'Non-VTD' as desk_source
                from bh_non_vtd
                where trunc(asofdate, 'MONTH') = v_nvtd_start_date
                and    non_vtd_rpl_code <> 'None'
                order by 1
                ) a
                where not exists (
                 select 1 from sdata_desk_attributes da where a.desk_rpl_code=da.desk_rpl_code and a.desk_source=da.desk_source
                 and nvl(a.exemp_excl_type,'NA') = nvl(da.exemp_excl_type,'NA') and nvl(a.control_type,'NA') = nvl(da.control_type,'NA')
                 and nvl(a.business,'NA') = nvl(da.business,'NA') and nvl(a.division,'NA') = nvl(da.division,'NA') and nvl(a.pvf,'NA') = nvl(da.pvf,'NA'))
                 )
        ) newdatt
    on (datt.desk_rpl_code =  newdatt.desk_rpl_code and datt.desk_source = newdatt.desk_source and datt.desk_source='Non-VTD' and datt.start_date <> newdatt.start_date)
    when matched then
        update set datt.end_date = newdatt.start_date -1, datt.status='Closed', update_datetime=sysdate, update_user='batch'
    ;

    dbms_output.put_line('Closed desks attributes for Non-VTD for old entries - rows : '||sql%rowcount);
    -- insert new changes with new effective date
    insert into sdata_desk_attributes (desk_rpl_code, volcker_relevant, metrics_reportable, exemp_excl_type, control_type, pvf,business, division, start_date, desk_source, update_user, update_datetime, status)
    select  a.desk_rpl_code, 'N' as volcker_relevant, 'N' as metrics_reportable, exemp_excl_type, control_type, pvf, business, division, start_date
              , desk_source ,'batch' as update_user, sysdate as update_datetime, 'Active' as status from
    (
    select trim(non_vtd_rpl_code) as desk_rpl_code, case when NON_VTD_EXCLUSION_TYPE='EP'  then 'EP' else 'NT' end as exemp_excl_type,
              case when NON_VTD_EXCLUSION_TYPE='EP'  then 'EP' else NON_VTD_EXCLUSION_TYPE end as control_type, non_vtd_pvf as pvf,
              non_vtd_business as business, non_vtd_division as division, v_nvtd_start_date as start_date, 'Non-VTD' as desk_source
    from bh_non_vtd
    where trunc(asofdate, 'MONTH') = v_nvtd_start_date
    and    non_vtd_rpl_code <> 'None'
    order by 1
    ) a
    where not exists (
     select 1 from sdata_desk_attributes da where a.desk_rpl_code=da.desk_rpl_code and a.desk_source=da.desk_source
     and nvl(a.exemp_excl_type,'NA') = nvl(da.exemp_excl_type,'NA') and nvl(a.control_type,'NA') = nvl(da.control_type,'NA')
     and nvl(a.business,'NA') = nvl(da.business,'NA') and nvl(a.division,'NA') = nvl(da.division,'NA') and nvl(a.pvf,'NA') = nvl(da.pvf,'NA'))
     ;
    dbms_output.put_line('Added new desks attributes for Non-VTD - rows : '||sql%rowcount);

     -- insert into audit table after update to close previous period
    -- insert into sdata_desk_attributes_aud
    -- select a.*, 'Update', a.rowid as orig_rowid  from sdata_desk_attributes a where desk_source='Non-VTD'
    -- and trunc(update_datetime)=trunc(sysdate);
    -- dbms_output.put_line('Added new desks attributes audit for Non-VTD - rows : '||sql%rowcount);

  -- check if any desk missing in brds_vw_vtd, but present in desk dimension
    select count(*)  into v_cnt_missing_desk
    from sdata_desk
    where (desk_rpl_code, desk_source) not in (select newd.desk_rpl_code, desk_source from
        (
            select volckertradingdesk as desk_rpl_code, 'bRDS-VTD' as desk_source from brds_vw_vtd union
            select non_vtd_rpl_code as desk_rpl_code, 'Non-VTD' as desk_source from bh_non_vtd where trunc(asofdate, 'MONTH') = v_nvtd_start_date union
            select rplcode as desk_rpl_code, 'Manual_Elems' as desk_source from bh_manual_hierarchy_elems
        ) newd
        where  newd.desk_rpl_code not in ('00_EXCLUDE','00_NO_MATCH','None')
        )
	and status <> 'Closed'
    ;
    dbms_output.put_line('Closed desks - rows : '||v_cnt_missing_desk);

	-- set status, end date for closed desk in desk and desk_attributes table
	update sdata_desk desk
	set      status='Closed', end_date=case when desk_source='Non-VTD' then v_nvtd_start_date-1 else v_vtd_start_date-1 end, update_user='batch', update_datetime=sysdate
	where (desk.desk_rpl_code, desk.desk_source) not in (select newd.desk_rpl_code, desk_source from
		(
			select volckertradingdesk as desk_rpl_code, 'bRDS-VTD' as desk_source from brds_vw_vtd union
			select non_vtd_rpl_code as desk_rpl_code, 'Non-VTD' as desk_source from bh_non_vtd  where trunc(asofdate, 'MONTH') = v_nvtd_start_date union
			select rplcode as desk_rpl_code, 'Manual_Elems' as desk_source from bh_manual_hierarchy_elems
		) newd
		where newd.desk_rpl_code not in ('00_EXCLUDE','00_NO_MATCH','None')
		)
	and status <> 'Closed' ;
    dbms_output.put_line('Updated closed desks - rows : '||sql%rowcount);

    update sdata_desk_attributes desk
    set      status='Closed', end_date=case when desk_source='Non-VTD' then v_nvtd_start_date-1 else v_vtd_start_date-1 end, update_user='batch', update_datetime=sysdate
    where (desk.desk_rpl_code, desk.desk_source) not in (select newd.desk_rpl_code, desk_source from
        (
            select volckertradingdesk as desk_rpl_code, 'bRDS-VTD' as desk_source from brds_vw_vtd union
            select non_vtd_rpl_code as desk_rpl_code, 'Non-VTD' as desk_source from bh_non_vtd  where trunc(asofdate, 'MONTH') = v_nvtd_start_date union
            select rplcode as desk_rpl_code, 'Manual_Elems' as desk_source from bh_manual_hierarchy_elems
        ) newd
        where newd.desk_rpl_code not in ('00_EXCLUDE','00_NO_MATCH','None')
        )
    and status <> 'Closed' and end_date is null;
    dbms_output.put_line('Updated closed desks in desk attributes - rows : '||sql%rowcount);

	-- populate legal entity table for any new legal entity from book_hierarchy_rpl table
    insert into sdata_legal_entity(legal_entity_id, legal_entity_name, legal_entity_short_name, compid, start_date, update_user, update_datetime)
    select (select max(legal_entity_id) from sdata_legal_entity)+rownum as legal_entity_id, legal_entity_name, legal_entity_name, compid, trunc(sysdate, 'MONTH') as start_date, 'batch' as update_user, sysdate as update_datetime
        from (
         select legal_entity as legal_entity_name, max(legal_entity_code) as compid
        from  book_hierarchy_rpl rpl
        where rpl.asofdate = (select max(rpl2.asofdate) from book_hierarchy_rpl rpl2)
        and nvl(legal_entity,'null') <> 'null'
		and nvl(rpl.volcker_reportable_flag, 'N') = 'Y'
        group by legal_entity
        ) newle
    where not exists (select 1 from sdata_legal_entity le where newle.legal_entity_name = le.legal_entity_name);
    dbms_output.put_line('Inserted legal entities - rows : '||sql%rowcount);

	-- update desk to legal entity map for new desks and new legal entity
    delete from sdata_desk_legal_entity_map;
    dbms_output.put_line('Deleted current desk to legal entity mapping - rows : '||sql%rowcount);
    insert into sdata_desk_legal_entity_map
    select distinct desk_rpl_code, legal_entity_id
    from (
    select distinct volcker_trading_desk as desk_rpl_code, legal_entity, le.legal_entity_id
    from book_hierarchy_rpl bh
    join sdata_legal_entity le on upper(bh.legal_entity) = upper(le.legal_entity_name)
    where legal_entity is not null and asofdate=v_nvtd_start_date -- look for legal entity of current month only GBSVR-35074
    and    volcker_trading_desk not in ('00_EXCLUDE','00_NO_MATCH')
    union
    select distinct non_vtd_rpl_code, legal_entity, le.legal_entity_id
    from  book_hierarchy_rpl bh
    join sdata_legal_entity le on upper(bh.legal_entity) = upper(le.legal_entity_name)
    and non_vtd_rpl_code is not null and asofdate=v_nvtd_start_date
    where legal_entity is not null
    );
    dbms_output.put_line('Added current desk to legal entity mapping - rows : '||sql%rowcount);

  -- update legal entity to regulator logic
    delete from sdata_legal_entity_regulator;
    dbms_output.put_line('Deleted current legal entity to regulator mapping - rows : '||sql%rowcount);
    insert into sdata_legal_entity_regulator
    select distinct legal_entity_id, regulator_id from (
    select legal_entity_id, legal_entity_name, regulator_id, regulator_code
    from  sdata_legal_entity, sdata_regulator
    where upper(legal_entity_name) = 'DEUTSCHE BANK TRUST COMPANY DELAWARE'
    and    regulator_code='FDIC'
    UNION ALL
    select legal_entity_id, legal_entity_name, regulator_id, regulator_code
    from  sdata_legal_entity, sdata_regulator
    where regulator_code='CFTC'
    and    (
                upper(legal_entity_name) like 'DEUTSCHE BANK AKTIENGESELLSCHAFT%' or
                upper(legal_entity_name) in ('DEUTSCHE BANK SECURITIES INC.','DEUTSCHE INTERNATIONAL CORPORATE SERVICES LIMITED','DEUTSCHE INVESTMENT MANAGEMENT AMERICAS INC.','RREEF AMERICA LLC','DB COMMODITY SERVICES LLC')
              )
    UNION ALL
    select legal_entity_id, legal_entity_name, regulator_id, regulator_code
    from  sdata_legal_entity, sdata_regulator
    where regulator_code='OCC'
    and upper(legal_entity_name) in ('DEUTSCHE BANK NATIONAL TRUST COMPANY','DEUTSCHE BANK TRUST COMPANY NEW JERSEY LTD.','DEUTSCHE BANK TRUST COMPANY, NATIONAL ASSOCIATION.')
    UNION ALL
    select legal_entity_id, legal_entity_name, regulator_id, regulator_code
    from  sdata_legal_entity, sdata_regulator
    where regulator_code='SEC'
    and upper(legal_entity_name) in (
                                                    'DB INVESTMENT MANAGERS, INC.',
                                                    'DBX ADVISORS LLC',
                                                    'DBX STRATEGIC ADVISORS LLC',
                                                    'DEUTSCHE INVESTMENT MANAGEMENT AMERICAS INC.',
                                                    'DEUTSCHE ALTERNATIVE ASSET MANAGEMENT (GLOBAL) LIMITED',
                                                    'DEUTSCHE ASSET MANAGEMENT (HONG KONG) LIMITED',
                                                    'DEUTSCHE ASSET MANAGEMENT INTERNATIONAL GMBH',
                                                    'DEUTSCHE BANK SECURITIES INC.',
                                                    'DEUTSCHE INVESTMENT MANAGEMENT AMERICAS INC.',
                                                    'DEUTSCHE INVESTMENTS AUSTRALIA LIMITED ',
                                                    'RREEF AMERICA LLC'
                                                    )

    )
    ;
   dbms_output.put_line('Added legal entity to regulator mapping - rows : '||sql%rowcount);

	----------------------------- Update UBR for new desk ----------------------------------------------------------------
	for desk_rec in (select desk_rpl_code, desk_source from sdata_desk where l5_ubr is null and status='Active')
	LOOP

		v_desk_rpl_code := desk_rec.desk_rpl_code;
		v_desk_source := desk_rec.desk_source;
		v_desk_l5ubr := null;
		BEGIN
			select nodename into v_desk_l5ubr from (
				select to_char(rownum) as UBR_DESK_LEVEL, NODENAME, RPLCODE, NODETYPE, NODEID, leafnodetype
				from
				(select  UBR_DESK_LEVEL, NODENAME, RPLCODE, NODETYPE, NODEID, leafnodetype
				from (
				select  h.nodeType,
						connect_by_root h.nodeId book,
						h.nodeId,
						h.nodeName,
						h.rplCode,
						level   ubr_desk_level,
						0 num_desks,
						0 num_ubrs,
						h.volckerTradingDesk,
						h.chargeReportingUnit,
						h.chargeReportingParent,
						connect_by_root h.nodetype leafnodetype
				from    brds_vw_hierarchy h
				connect by prior parentNodeId = nodeId
				)
				where book in (select desk_id from sdata_desk where desk_rpl_code=v_desk_rpl_code and desk_source=v_desk_source)
				and   leafnodetype in (select nodetype from brds_vw_hierarchy where rplcode=v_desk_rpl_code)
				order by 1 desc
				)
				) where UBR_DESK_LEVEL=5;

			update sdata_desk set l5_UBR=v_desk_l5ubr where desk_rpl_code=v_desk_rpl_code and desk_source=v_desk_source;
			dbms_output.put_line('Updated L5_UBR :'|| v_desk_l5ubr ||' for desk : '|| v_desk_rpl_code);
			EXCEPTION
			 WHEN NO_DATA_FOUND THEN
				dbms_output.put_line('No L5_UBR data found for desk : '||v_desk_rpl_code);
		END;

	END LOOP;

	----------------------------- Finally set elapsed month to current month if not done already -------------------------
	update sdata_month set vri_elapsed_month='Y' where month_id=v_current_month_id and vri_elapsed_month<>'Y';

	----------------------------- Replace VRI_DESK_MAPPING data for current month as data might have changed due to regulator mapping --------
    if ( v_enable_vri_upd = 'Enable')
    then
        delete from VRI_DESK_MAPPING where asofdate=v_nvtd_start_date
        and desk_id not in (select desk_id from vw_sdata_vri_desk_mapping vw where vw.asofdate=v_nvtd_start_date and metrics_reportable='Y');
        dbms_output.put_line('Delete invalid vri_desk_mapping for current month - rows : '||sql%rowcount);

        merge into vri_desk_mapping vrid
        using (
                select (select max(ID) from VRI_DESK_MAPPING)+rownum ID, DESK_ID, DESK_NAME, AGENCY_ID, DESK_DESCRIPTION, ASOFDATE, METRICS_MEETING, REGION, COUNTRY, PVF, BUSINESS, DIVISION, CF_DESK_FLAG,
						DESK_UBR_ID, EXEMP_EXCL, INTENT,DESK_BUSINESS, DESK_DIVISION, VRO_LIQUIDITY_PROFILE, VRO_CREDIT_NONCREDIT, DESK_SOURCE, METRICS_REPORTABLE
                from vw_sdata_vri_desk_mapping where asofdate=v_nvtd_start_date and metrics_reportable='Y'
                ) newvrid
        on  (vrid.desk_id = newvrid.desk_id and vrid.agency_id = newvrid.agency_id and vrid.asofdate = newvrid.asofdate)
        when matched then update set
        vrid.DESK_NAME = newvrid.DESK_NAME, vrid.DESK_DESCRIPTION = newvrid.DESK_DESCRIPTION,vrid.METRICS_MEETING = newvrid.METRICS_MEETING, vrid.REGION = newvrid.REGION,
        vrid.COUNTRY = newvrid.COUNTRY, vrid.PVF = newvrid.PVF, vrid.BUSINESS = newvrid.BUSINESS, vrid.DIVISION = newvrid.DIVISION, vrid.CF_DESK_FLAG = newvrid.CF_DESK_FLAG,
		vrid.DESK_UBR_ID = newvrid.DESK_UBR_ID, vrid.EXEMP_EXCL = newvrid.EXEMP_EXCL, vrid.INTENT = newvrid.INTENT,vrid.DESK_BUSINESS = newvrid.DESK_BUSINESS, vrid.DESK_DIVISION = newvrid.DESK_DIVISION, 
		vrid.VRO_LIQUIDITY_PROFILE = newvrid.VRO_LIQUIDITY_PROFILE, vrid.VRO_CREDIT_NONCREDIT = newvrid.VRO_CREDIT_NONCREDIT, vrid.DESK_SOURCE = newvrid.DESK_SOURCE, vrid.METRICS_REPORTABLE = newvrid.METRICS_REPORTABLE
        when not matched then insert(ID,DESK_ID,DESK_NAME,AGENCY_ID,DESK_DESCRIPTION,ASOFDATE,METRICS_MEETING,REGION,COUNTRY,PVF,BUSINESS,DIVISION,CF_DESK_FLAG,
		DESK_UBR_ID, EXEMP_EXCL, INTENT,DESK_BUSINESS, DESK_DIVISION, VRO_LIQUIDITY_PROFILE, VRO_CREDIT_NONCREDIT, DESK_SOURCE, METRICS_REPORTABLE)
        values (newvrid.ID,newvrid.DESK_ID,newvrid.DESK_NAME,newvrid.AGENCY_ID,newvrid.DESK_DESCRIPTION,newvrid.ASOFDATE,newvrid.METRICS_MEETING,newvrid.REGION,
        newvrid.COUNTRY,newvrid.PVF,newvrid.BUSINESS,newvrid.DIVISION,newvrid.CF_DESK_FLAG,
		newvrid.DESK_UBR_ID, newvrid.EXEMP_EXCL, newvrid.INTENT,newvrid.DESK_BUSINESS, newvrid.DESK_DIVISION, newvrid.VRO_LIQUIDITY_PROFILE, newvrid.VRO_CREDIT_NONCREDIT, newvrid.DESK_SOURCE, newvrid.METRICS_REPORTABLE)
        ;
        dbms_output.put_line('Synched up new changes with vri_desk_mapping for current month - rows : '||sql%rowcount);
    end if;
	----------------------------- Replace VRI_ data for current month with static data current snapshot when process is enalbed --------
	if nvl(v_enable_rfs,'Disable')='Enable' then
        -- update vri_desk_sensitivity_mapping
        delete from vri_desk_sensitivity_mapping where trunc(asofdate,'mm')=trunc(sysdate,'mm');
        dbms_output.put_line('Deleted '||sql%rowcount||' rows from vri_desk_sensitivity_mapping');
        insert into vri_desk_sensitivity_mapping(ID, asofdate, desk_code, sensitivity_id, limit, prime_flag)
        select (select max(ID) from vri_desk_sensitivity_mapping)+ID as ID, asofdate, desk_code, sensitivity_id, limit, prime_flag  from VW_SDATA_DESK_SENSITIVITY_MAP;
        dbms_output.put_line('Added '||sql%rowcount||' rows into vri_desk_sensitivity_mapping');
        -- update vri_sensitivity_att_mapping
        delete from vri_sensitivity_att_mapping where trunc(asofdate,'mm')=trunc(sysdate,'mm');
        dbms_output.put_line('Deleted '||sql%rowcount||' rows from vri_sensitivity_att_mapping');
        insert into vri_sensitivity_att_mapping(ID, asofdate, sentivity, risk_fator_sensitivity_name, risk_factor_change, risk_factor_change_units, risk_class_method,measurement_unit, category_id, category_name)
        select (select max(ID) from vri_sensitivity_att_mapping)+ID as ID, asofdate, sentivity, risk_fator_sensitivity_name, risk_factor_change, risk_factor_change_units, risk_class_method,measurement_unit, category_id, category_name  from VW_SDATA_SENSITIVITY_ATT_MAP;
        dbms_output.put_line('Added '||sql%rowcount||' rows into vri_sensitivity_att_mapping');
        -- update vri_asset_allocation_mapping
        delete from vri_asset_allocation_mapping where trunc(asofdate,'mm')=trunc(sysdate,'mm');
        dbms_output.put_line('Deleted '||sql%rowcount||' rows from vri_asset_allocation_mapping');
        insert into vri_asset_allocation_mapping(ID, asofdate, asset_allocation, mapping_value, measurement_unit)
        select (select max(ID) from vri_asset_allocation_mapping)+ID as ID, asofdate, asset_allocation, mapping_value,measurement_unit  from VW_SDATA_ASSET_ALLOC_MAP;
        dbms_output.put_line('Added '||sql%rowcount||' rows into vri_asset_allocation_mapping');
    end if;
    dbms_output.put_line('Finished the job at : '||to_char(sysdate,'DD-MON-YYYY HH24:MI:SS'));
    COMMIT;
EXCEPTION  -- exception handlers begin

   WHEN OTHERS THEN  -- handles all other errors
   DBMS_OUTPUT.PUT_LINE('P_PROCESS_DESKATTR: ' ||' - '||sqlcode || sqlerrm);
   ROLLBACK;
END P_PROCESS_DESKATTR;
END PKG_SDATA_PROCESS_DESK;