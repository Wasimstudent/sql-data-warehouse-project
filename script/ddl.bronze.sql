
EXEC bronze.load_bronze
==================================================================================================================================
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
Begin
     DECLARE @Start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	 Begin Try
	    SET @batch_start_time = GETDATE();
		PRINT'==================================================';
		PRINT 'Loading Bronze';
		PRINT'==================================================';

		PRINT '-------------------------------------------------';
		PRINT 'Loading CRM table';
		PRINT '-------------------------------------------------';

		SET @Start_time = GETDATE();
		Print'>> Truncating Table: crm_cust_info';
		Truncate Table bronze.crm_cust_info;
        Print'>> Inserting Data Into: crm_cust_info';

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\WASH1\OneDrive - Mondia Media Germany GmbH\Desktop\SQL\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH  (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second';
		Print'=================================================================================================';

		SET @Start_time = GETDATE();
		Print'>> Truncating Table: crm_prd_info';
		Truncate Table bronze.crm_prd_info;
		Print'>> Inserting Data Into:  crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\WASH1\OneDrive - Mondia Media Germany GmbH\Desktop\SQL\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH  (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second';
		Print'=================================================================================================';

		SET @Start_time = GETDATE();
		Print'>> Truncating Table: crm_prd_info';
		Truncate Table bronze.crm_sales_details;
		Print'>> Inserting Data Into:crm_prd_info';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\WASH1\OneDrive - Mondia Media Germany GmbH\Desktop\SQL\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH  (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second';
		Print'=================================================================================================';


		PRINT '-------------------------------------------------';
		PRINT 'Loading CRM table';
		PRINT '-------------------------------------------------';

		SET @Start_time = GETDATE();
		Print'>> Truncating Table:erp_cust_az12';
		Truncate Table bronze.erp_cust_az12;
		Print'>> Inserting Data Into:erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\WASH1\OneDrive - Mondia Media Germany GmbH\Desktop\SQL\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH  (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second';
		Print'=================================================================================================';

		SET @Start_time = GETDATE();
		Print'>> Truncating Table:erp_loc_a101';
		Truncate Table bronze.erp_loc_a101;
		Print'>> Inserting Data Into:erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\WASH1\OneDrive - Mondia Media Germany GmbH\Desktop\SQL\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH  (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second';
		Print'=================================================================================================';

		SET @Start_time = GETDATE();
		Print'>> Truncating Table:erp_px_cat_g1v2';
		Truncate Table bronze.erp_px_cat_g1v2;
		Print'>> Inserting Data Into:erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\WASH1\OneDrive - Mondia Media Germany GmbH\Desktop\SQL\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH  (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second';
		Print'=================================================================================================';
		SET @batch_end_time = GETDATE();
		PRINT'Loading Bronze Layer is completed';
		PRINT'    -Total Load Duration: ' + Cast(DATEDIFF(second, @batch_start_time,@batch_end_time) As NVARCHAR) + ' second';
		PRINT'===========================================================';
	 End try
	 begin catch
	 PRINT'=======================================================';
	 PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
	 PRINT'ERROR Message' + ERROR_MESSAGE();
	 PRINT'ERROR Message' + CAST (ERROR_NUMBER()AS NVARCHAR);
	 PRINT'ERROR Message' + CAST (ERROR_STATE()AS NVARCHAR);
	 PRINT'=======================================================';
	 end catch
END
