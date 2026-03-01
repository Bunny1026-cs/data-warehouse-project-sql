CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    
    BEGIN TRY

        SET @start_time = GETDATE();
        SET @batch_start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Loading Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' secs'; 


        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Loading Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' secs'; 


        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );


        SET @end_time = GETDATE();
        PRINT 'Loading Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' secs';

        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Loading Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' secs';

        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Loading Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' secs';

        SET @start_time = GETDATE();

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\dell\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Loading Duration - ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' secs';


        SET @batch_end_time = GETDATE();
        PRINT 'Total Loading Duration - ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' secs'; 

    END TRY
    BEGIN CATCH
        PRINT 'Error Message : ' + ERROR_MESSAGE();
    END CATCH
END
