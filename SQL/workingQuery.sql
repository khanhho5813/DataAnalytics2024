select 
		--state_name,county_name,
		-- applicant_ethnicity_name,
		applicant_race_NAME_1,
		-- denial_reason_name_1,
		avg(cast(loan_amount_000s as float)) as loan, 
		avg(CAST(applicant_income_000s AS FLOAT)) as income,
		avg(cast(rate_spread as float)) as spread,
		cont(*)
from "CRDB".hmda_2017_nationwide_allrecords_labels
where denial_reason_1 = ''
group by 
		--state_name,county_name,
		--applicant_ethnicity_name,
		applicant_race_NAME_1

-- ----------------------------------------------

select 
		--state_name,county_name,
		applicant_ethnicity_name,
		applicant_race_NAME_1,
		denial_reason_1,
		-- denial_reason_name_1,
		count(*)
from "CRDB".hmda_2017_nationwide_allrecords_labels
group by 
		--state_name,county_name,
		applicant_ethnicity_name,
		applicant_race_NAME_1,
		denial_reason_1

/*
update "CRDB".hmda_2017_nationwide_allrecords_labels
set rate_spread = null where rate_spread = ''
*/