create view srh_edifecs_dev.tx_tchp_encounters_00 as 

select
	'00' as c_00_header_record,
	'EMCSV' as c_emcsv,
	'1.5.3' as c_version,
	"date_part" ('epoch'::text, getdate()) as c_fileid,
	'Medical' as c_file_type,
	to_char(getdate(), 'YYYYMMDDHHMMSS'::character varying::text) as c_filecreationdatetime,
	'SAFERIDE01' as c_senderid,
	'EDIFECSTCHTXCD' as c_receiverid,
	'617591011TEDP' as c_destinationid,
	'P' as c_mode,
	'' as c_historicaltransmissionreceiveddate
	
	;