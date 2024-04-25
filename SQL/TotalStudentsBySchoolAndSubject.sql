/*
This query returns the total number of students by high school, and each subject area
*/

select E.lea_state, E.leaid, E.lea_name, E.schid, E.sch_name,  
		cast(tot_enr_m as integer) + cast(tot_enr_f as integer) as TOTAL,
		cast(tot_mathenr_advm_m as integer) + cast(tot_mathenr_advm_f as integer) as ADVMATH
from 	"CRDB".schoolcharacteristics SC
		join "CRDB".enrollment E 
		on SC.schid = E.schid and SC.leaid = E.leaid
		join "CRDB".advancedmathematics AM
		on AM.schid = E.schid and AM.leaid = E.leaid
where	sch_grade_g09 = 'Yes'
		and sch_grade_g10  = 'Yes'
		and sch_grade_g11 = 'Yes'
		and sch_grade_g12 = 'Yes'
		and cast(tot_enr_m as integer) > 0
		and cast(tot_enr_f as integer) > 0
		and cast(tot_mathenr_advm_m as integer) > 0
		and cast(tot_mathenr_advm_f as integer) > 0
order by E.lea_state, E.lea_name, E.sch_name
limit 100;