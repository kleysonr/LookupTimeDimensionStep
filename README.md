LookupTimeDimensionStep
=======================

LookupTimeDimension is a new step for Kettle transformation to lookup/create an entry on DW dimension time table and return the id.

The step will populate, based on a LOCALE settings, a time dimension with the following fields:

	* id
	* is_last_day_of_month 
	* year_semester 
	* semester_number
	* month_name
	* month_abbreviation
	* week_in_month
	* is_first_day_of_month
	* week_in_year
	* bimonthly_name
	* date_medium
	* quarter_number
	* is_first_day_in_week
	* day_in_year
	* month_number
	* bimonthly_number
	* semester_name
	* year_bimonthly
	* year_quarter
	* is_last_day_in_week
	* day_name
	* date_short
	* year_month_number
	* quarter_name
	* date_full
	* year_of_week
	* year_month_abbreviation
	* day_in_month
	* day_abbreviation
	* year4
	* date_long 


HOW TO USE
----------

The plugin lookup and/or create a new key based on the fields defined on the field table.
Some combinations are allowed and take precedence in the following order:

	year - month - day	// Complete date
	year - month		// Set the first day of the month
	year - week			// Calculate the month of the week and set the first day of th month
	year - bimonthly	// Set the first day of the first month of the bimonthly
	year - quarter		// Set the first day of the first month of the quarter
	year - semester		// Set the first day of the first month of the semester
	year				// Set the first day of the year
	

Set 'Enable year null' checkbox if you have any data without date. This will create the id 0.
Set 'Only Lookup' checkbox to avoid gaps on the date dimension. A preload is necessary to populate the dimension.


HOW TO INSTALL
--------------

Download the plugin at http://sourceforge.net/projects/lookuptimedimension/files/LookupTimeDimensionStep-1.0.0.zip/download or use the Marketplace.


SAMPLE
------

Download a sample at http://sourceforge.net/projects/lookuptimedimension/files/load_dim_date.ktr/download