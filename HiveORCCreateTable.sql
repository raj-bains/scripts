drop schema if exists testv1_raw;
create schema testv1_raw;
drop table if exists testv1_raw.TestV1_Batters;
CREATE TABLE testv1_raw.TestV1_Batters (
      Player STRING ,
      Team STRING ,
      League STRING ,
      Year SMALLINT,
      Games DOUBLE,
      AB DOUBLE,
      R DOUBLE,
      H DOUBLE,
      Doubles DOUBLE,
      Triples DOUBLE,
      HR DOUBLE,
      RBI DOUBLE,
      SB DOUBLE,
      CS DOUBLE,
      BB DOUBLE,
      SO DOUBLE,
      IBB DOUBLE,
      HBP DOUBLE,
      SH DOUBLE,
      SF DOUBLE,
      GIDP DOUBLE
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/Batters000.tbl' INTO TABLE testv1_raw.TestV1_Batters;
drop table if exists testv1_raw.TestV1_Calcs;
CREATE TABLE testv1_raw.TestV1_Calcs (
      key STRING,
      num0 DOUBLE,
      num1 DOUBLE,
      num2 DOUBLE,
      num3 DOUBLE,
      num4 DOUBLE,
      str0 STRING,
      str1 STRING,
      str2 STRING,
      str3 STRING,
      int0 INT,
      int1 INT,
      int2 INT,
      int3 INT,
      bool0 BOOLEAN,
      bool1 BOOLEAN,
      bool2 BOOLEAN,
      bool3 BOOLEAN,
      date0 STRING,
      date1 STRING,
      date2 STRING,
      date3 STRING,
      time0 STRING,
      time1 STRING,
      datetime0 STRING,
      datetime1 STRING,
      zzz STRING
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/Calcs000.tbl' INTO TABLE testv1_raw.TestV1_Calcs;
drop table if exists testv1_raw.TestV1_DateBins;
CREATE TABLE testv1_raw.TestV1_DateBins (
        Key STRING,
        Date_ STRING,
        Value DOUBLE
        );
LOAD DATA LOCAL INPATH '/tmp/TestV1/DateBins000.tbl' INTO TABLE testv1_raw.TestV1_DateBins;
drop table if exists testv1_raw.TestV1_DateTime;
CREATE TABLE testv1_raw.TestV1_DateTime (
      ID INT,
      Time STRING,
      Date_ STRING,
      `DateTime` STRING,
      Random_ DOUBLE,
      Diagonal DOUBLE,
      Curved DOUBLE
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/DateTime000.tbl' INTO TABLE testv1_raw.TestV1_DateTime;
drop table if exists testv1_raw.TestV1_Election;
CREATE TABLE testv1_raw.TestV1_Election (
      PointOrder INT,
      Longitude DOUBLE,
      Latitude DOUBLE,
      PolygonID STRING ,
      State STRING ,
      Pop_SQ_Mile DOUBLE,
      Elected STRING ,
      Candidate STRING ,
      Votes DOUBLE,
      MarginOfVictory STRING
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/Election000.tbl' INTO TABLE testv1_raw.TestV1_Election;
drop table if exists testv1_raw.TestV1_FischerIris;
CREATE TABLE testv1_raw.TestV1_FischerIris (
      ID DOUBLE,
      Case_ DOUBLE,
      Species_No DOUBLE,
      Species STRING ,
      Organ STRING ,
      Width DOUBLE,
      Length DOUBLE
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/FischerIris000.tbl' INTO TABLE testv1_raw.TestV1_FischerIris;
drop table if exists testv1_raw.TestV1_Loan;
CREATE TABLE testv1_raw.TestV1_Loan (
      Loan_Decision STRING ,
      Loan_Status STRING ,
      Loan_Amt INT,
      App_Prior_Customer STRING ,
      App_Race STRING ,
      App_Gender STRING ,
      App_Age INT,
      App_Trail_Income DOUBLE,
      Loan_Region INT,
      App_Num_Homes INT,
      App_Married_Status STRING ,
      App_Employ_Status STRING ,
      App_Yrs_Current_Job DOUBLE,
      App_Self_Emp STRING ,
      App_Tot_Month_Inc DOUBLE,
      CoApp_Tot_Month_Inc DOUBLE,
      App_Ext_Credit_Rating DOUBLE,
      Loan_Price DOUBLE,
      App_Liquid_Assets DOUBLE,
      App_Num_Mortgages INT,
      App_Negative_Flags INT,
      Property_Public_Rec INT,
      App_Int_Credit_Rating_1 DOUBLE,
      App_Int_Credit_Rating_2 DOUBLE,
      Loan_Adjustable STRING ,
      Loan_Term INT,
      App_Gift_Income STRING ,
      App_CoSign STRING ,
      App_Net_North DOUBLE,
      App_College STRING ,
      App_Mortgage_Perform STRING ,
      Loan_Multi_Property STRING ,
      Loan_Pct_Project DOUBLE
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/Loan000.tbl' INTO TABLE testv1_raw.TestV1_Loan;
drop table if exists testv1_raw.TestV1_NumericBins;
CREATE TABLE testv1_raw.TestV1_NumericBins (
      Key_ STRING ,
      Measure_Em2 DOUBLE,
      Measure_Em1 DOUBLE,
      Measure_Ep0 DOUBLE,
      Measure_Ep1 DOUBLE,
      Measure_Ep4 DOUBLE,
      Measure_Ep7 DOUBLE
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/NumericBins000.tbl' INTO TABLE testv1_raw.TestV1_NumericBins;
drop table if exists testv1_raw.TestV1_REI;
CREATE TABLE testv1_raw.TestV1_REI (
      Date_ STRING,
      Category1 STRING ,
      Category2 STRING ,
      Category3 STRING ,
      Category4 STRING ,
      Category5 STRING ,
      Trans_Description STRING ,
      Total_Units DOUBLE,
      Total_Sale_Revenue DOUBLE,
      Total_Sale_Cost DOUBLE,
      Total_Sale_Margin DOUBLE
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/REI000.tbl' INTO TABLE testv1_raw.TestV1_REI;
drop table if exists testv1_raw.TestV1_SeattleCrime;
CREATE TABLE testv1_raw.TestV1_SeattleCrime (
      YEAR DOUBLE,
      MONTH STRING ,
      PRECINCT STRING ,
      TRACT STRING ,
      SAFE STRING ,
      MURDER DOUBLE,
      RAPE DOUBLE,
      ROBBERY DOUBLE,
      AGG_ASSAULT DOUBLE,
      RES_BURGLARY DOUBLE,
      NON_RES_BURGLARY DOUBLE,
      THEFT DOUBLE,
      AUTOTHEFT DOUBLE,
      ARSON DOUBLE,
      TOTAL DOUBLE
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/SeattleCrime000.tbl' INTO TABLE testv1_raw.TestV1_SeattleCrime;
drop table if exists testv1_raw.TestV1_Securities;
CREATE TABLE testv1_raw.TestV1_Securities (
      Date_ STRING,
      Ticker STRING ,
      Open_Price DOUBLE,
      High_Price DOUBLE,
      Low_Price DOUBLE,
      Close_Price DOUBLE,
      Volume DOUBLE,
      Industry STRING ,
      Recommendation STRING ,
      Big_Move STRING ,
      Insider STRING
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/Securities000.tbl' INTO TABLE testv1_raw.TestV1_Securities;
drop table if exists testv1_raw.TestV1_SpecialData;
CREATE TABLE testv1_raw.TestV1_SpecialData (
      keyName STRING ,
      val1 INT,
      val2 INT,
      val3 INT,
      order_ INT,
      val4 INT,
      val5 INT
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/SpecialData000.tbl' INTO TABLE testv1_raw.TestV1_SpecialData;
drop table if exists testv1_raw.TestV1_Staples;
CREATE TABLE testv1_raw.TestV1_Staples (
      Item_Count INT,
      Ship_Priority STRING,
      Order_Priority STRING,
      Order_Status STRING,
      Order_Quantity DOUBLE,
      Sales_Total DOUBLE,
      Discount DOUBLE,
      Tax_Rate DOUBLE,
      Ship_Mode STRING,
      Fill_Time DOUBLE,
      Gross_Profit DOUBLE,
      Price DOUBLE,
      Ship_Handle_Cost DOUBLE,
      Employee_Name STRING,
      Employee_Dept STRING,
      Manager_Name STRING,
      Employee_Yrs_Exp DOUBLE,
      Employee_Salary DOUBLE,
      Customer_Name STRING,
      Customer_State STRING,
      Call_Center_Region STRING,
      Customer_Balance DOUBLE,
      Customer_Segment STRING,
      Prod_Type1 STRING,
      Prod_Type2 STRING,
      Prod_Type3 STRING,
      Prod_Type4 STRING,
      Product_Name STRING,
      Product_Container STRING,
      Ship_Promo STRING,
      Supplier_Name STRING,
      Supplier_Balance DOUBLE,
      Supplier_Region STRING,
      Supplier_State STRING,
      Order_ID STRING,
      Order_Year INT,
      Order_Month INT,
      Order_Day INT,
      Order_Date_ STRING,
      Order_Quarter STRING,
      Product_Base_Margin DOUBLE,
      Product_ID STRING,
      Receive_Time DOUBLE,
      Received_Date_ STRING,
      Ship_Date_ STRING,
      Ship_Charge DOUBLE,
      Total_Cycle_Time DOUBLE,
      Product_In_Stock STRING,
      PID INT,
      Market_Segment STRING
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/Staples000.tbl' INTO TABLE testv1_raw.TestV1_Staples;
drop table if exists testv1_raw.TestV1_Starbucks;
CREATE TABLE testv1_raw.TestV1_Starbucks (
      Market STRING ,
      State STRING ,
      Market_Size STRING ,
      Product_Type STRING ,
      Product STRING ,
      Type STRING ,
      Profit DOUBLE,
      Margin DOUBLE,
      Sales DOUBLE,
      COGS DOUBLE,
      Total_Expenses DOUBLE,
      Marketing DOUBLE,
      Payroll DOUBLE,
      Misc DOUBLE,
      Inventory DOUBLE,
      Opening DOUBLE,
      Additions DOUBLE,
      Ending DOUBLE,
      Margin_Rate DOUBLE,
      Profit_Ratio DOUBLE,
      Budget_Profit DOUBLE,
      Budget_Margin DOUBLE,
      Budget_Sales DOUBLE,
      Budget_COGS DOUBLE,
      Budget_Payroll DOUBLE,
      Budget_Additions DOUBLE,
      Item_Count BIGINT,
      Date_ STRING
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/Starbucks000.tbl' INTO TABLE testv1_raw.TestV1_Starbucks;
drop table if exists testv1_raw.TestV1_UTStarcom;
CREATE TABLE testv1_raw.TestV1_UTStarcom (
      Account_Type1 STRING ,
      Account_Type2 STRING ,
      Func_Descrip STRING ,
      Location_Description STRING ,
      Date_ STRING,
      Income_State_Prop STRING ,
      Scenario_Type2 STRING ,
      Measure DOUBLE
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/UTStarcom000.tbl' INTO TABLE testv1_raw.TestV1_UTStarcom;
drop table if exists testv1_raw.TestV1_xy;
CREATE TABLE testv1_raw.TestV1_xy (
      a STRING,
      x INT,
      y INT
      );
LOAD DATA LOCAL INPATH '/tmp/TestV1/xy000.tbl' INTO TABLE testv1_raw.TestV1_xy;
drop table if exists TestV1_Batters;
CREATE TABLE TestV1_Batters (
      Player STRING ,
      Team STRING ,
      League STRING ,
      Year SMALLINT,
      Games DOUBLE,
      AB DOUBLE,
      R DOUBLE,
      H DOUBLE,
      Doubles DOUBLE,
      Triples DOUBLE,
      HR DOUBLE,
      RBI DOUBLE,
      SB DOUBLE,
      CS DOUBLE,
      BB DOUBLE,
      SO DOUBLE,
      IBB DOUBLE,
      HBP DOUBLE,
      SH DOUBLE,
      SF DOUBLE,
      GIDP DOUBLE
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TestV1_Calcs;
CREATE TABLE TestV1_Calcs (
      key STRING,
      num0 DOUBLE,
      num1 DOUBLE,
      num2 DOUBLE,
      num3 DOUBLE,
      num4 DOUBLE,
      str0 STRING,
      str1 STRING,
      str2 STRING,
      str3 STRING,
      int0 INT,
      int1 INT,
      int2 INT,
      int3 INT,
      bool0 BOOLEAN,
      bool1 BOOLEAN,
      bool2 BOOLEAN,
      bool3 BOOLEAN,
      date0 STRING,
      date1 STRING,
      date2 STRING,
      date3 STRING,
      time0 STRING,
      time1 STRING,
      datetime0 STRING,
      datetime1 STRING,
      zzz STRING
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TestV1_DateBins;
CREATE TABLE TestV1_DateBins (
        Key STRING,
        Date_ STRING,
        Value DOUBLE
        )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TesTV1_DateTime;
CREATE TABLE TestV1_DateTime (
      ID INT,
      Time STRING,
      Date_ STRING,
      `DateTime` STRING,
      Random_ DOUBLE,
      Diagonal DOUBLE,
      Curved DOUBLE
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TestV1_Election;
CREATE TABLE TestV1_Election (
      PointOrder INT,
      Longitude DOUBLE,
      Latitude DOUBLE,
      PolygonID STRING ,
      State STRING ,
      Pop_SQ_Mile DOUBLE,
      Elected STRING ,
      Candidate STRING ,
      Votes DOUBLE,
      MarginOfVictory STRING
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TestV1_FischerIris;
CREATE TABLE TestV1_FischerIris (
      ID DOUBLE,
      Case_ DOUBLE,
      Species_No DOUBLE,
      Species STRING ,
      Organ STRING ,
      Width DOUBLE,
      Length DOUBLE
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TestV1_Loan;
CREATE TABLE TestV1_Loan (
      Loan_Decision STRING ,
      Loan_Status STRING ,
      Loan_Amt INT,
      App_Prior_Customer STRING ,
      App_Race STRING ,
      App_Gender STRING ,
      App_Age INT,
      App_Trail_Income DOUBLE,
      Loan_Region INT,
      App_Num_Homes INT,
      App_Married_Status STRING ,
      App_Employ_Status STRING ,
      App_Yrs_Current_Job DOUBLE,
      App_Self_Emp STRING ,
      App_Tot_Month_Inc DOUBLE,
      CoApp_Tot_Month_Inc DOUBLE,
      App_Ext_Credit_Rating DOUBLE,
      Loan_Price DOUBLE,
      App_Liquid_Assets DOUBLE,
      App_Num_Mortgages INT,
      App_Negative_Flags INT,
      Property_Public_Rec INT,
      App_Int_Credit_Rating_1 DOUBLE,
      App_Int_Credit_Rating_2 DOUBLE,
      Loan_Adjustable STRING ,
      Loan_Term INT,
      App_Gift_Income STRING ,
      App_CoSign STRING ,
      App_Net_North DOUBLE,
      App_College STRING ,
      App_Mortgage_Perform STRING ,
      Loan_Multi_Property STRING ,
      Loan_Pct_Project DOUBLE
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TestV1_NumericBins;
CREATE TABLE TestV1_NumericBins (
      Key_ STRING ,
      Measure_Em2 DOUBLE,
      Measure_Em1 DOUBLE,
      Measure_Ep0 DOUBLE,
      Measure_Ep1 DOUBLE,
      Measure_Ep4 DOUBLE,
      Measure_Ep7 DOUBLE
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TestV1_REI;
CREATE TABLE TestV1_REI (
      Date_ STRING,
      Category1 STRING ,
      Category2 STRING ,
      Category3 STRING ,
      Category4 STRING ,
      Category5 STRING ,
      Trans_Description STRING ,
      Total_Units DOUBLE,
      Total_Sale_Revenue DOUBLE,
      Total_Sale_Cost DOUBLE,
      Total_Sale_Margin DOUBLE
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TestV1_SeattleCrime;
CREATE TABLE TestV1_SeattleCrime (
      YEAR DOUBLE,
      MONTH STRING ,
      PRECINCT STRING ,
      TRACT STRING ,
      SAFE STRING ,
      MURDER DOUBLE,
      RAPE DOUBLE,
      ROBBERY DOUBLE,
      AGG_ASSAULT DOUBLE,
      RES_BURGLARY DOUBLE,
      NON_RES_BURGLARY DOUBLE,
      THEFT DOUBLE,
      AUTOTHEFT DOUBLE,
      ARSON DOUBLE,
      TOTAL DOUBLE
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TestV1_Securities;
CREATE TABLE TestV1_Securities (
      Date_ STRING,
      Ticker STRING ,
      Open_Price DOUBLE,
      High_Price DOUBLE,
      Low_Price DOUBLE,
      Close_Price DOUBLE,
      Volume DOUBLE,
      Industry STRING ,
      Recommendation STRING ,
      Big_Move STRING ,
      Insider STRING
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TestV1_SpecialData;
CREATE TABLE TestV1_SpecialData (
      keyName STRING ,
      val1 INT,
      val2 INT,
      val3 INT,
      order_ INT,
      val4 INT,
      val5 INT
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TestV1_Staples;
CREATE TABLE TestV1_Staples (
      Item_Count INT,
      Ship_Priority STRING,
      Order_Priority STRING,
      Order_Status STRING,
      Order_Quantity DOUBLE,
      Sales_Total DOUBLE,
      Discount DOUBLE,
      Tax_Rate DOUBLE,
      Ship_Mode STRING,
      Fill_Time DOUBLE,
      Gross_Profit DOUBLE,
      Price DOUBLE,
      Ship_Handle_Cost DOUBLE,
      Employee_Name STRING,
      Employee_Dept STRING,
      Manager_Name STRING,
      Employee_Yrs_Exp DOUBLE,
      Employee_Salary DOUBLE,
      Customer_Name STRING,
      Customer_State STRING,
      Call_Center_Region STRING,
      Customer_Balance DOUBLE,
      Customer_Segment STRING,
      Prod_Type1 STRING,
      Prod_Type2 STRING,
      Prod_Type3 STRING,
      Prod_Type4 STRING,
      Product_Name STRING,
      Product_Container STRING,
      Ship_Promo STRING,
      Supplier_Name STRING,
      Supplier_Balance DOUBLE,
      Supplier_Region STRING,
      Supplier_State STRING,
      Order_ID STRING,
      Order_Year INT,
      Order_Month INT,
      Order_Day INT,
      Order_Date_ STRING,
      Order_Quarter STRING,
      Product_Base_Margin DOUBLE,
      Product_ID STRING,
      Receive_Time DOUBLE,
      Received_Date_ STRING,
      Ship_Date_ STRING,
      Ship_Charge DOUBLE,
      Total_Cycle_Time DOUBLE,
      Product_In_Stock STRING,
      PID INT,
      Market_Segment STRING
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TestV1_Starbucks;
CREATE TABLE TestV1_Starbucks (
      Market STRING ,
      State STRING ,
      Market_Size STRING ,
      Product_Type STRING ,
      Product STRING ,
      Type STRING ,
      Profit DOUBLE,
      Margin DOUBLE,
      Sales DOUBLE,
      COGS DOUBLE,
      Total_Expenses DOUBLE,
      Marketing DOUBLE,
      Payroll DOUBLE,
      Misc DOUBLE,
      Inventory DOUBLE,
      Opening DOUBLE,
      Additions DOUBLE,
      Ending DOUBLE,
      Margin_Rate DOUBLE,
      Profit_Ratio DOUBLE,
      Budget_Profit DOUBLE,
      Budget_Margin DOUBLE,
      Budget_Sales DOUBLE,
      Budget_COGS DOUBLE,
      Budget_Payroll DOUBLE,
      Budget_Additions DOUBLE,
      Item_Count BIGINT,
      Date_ STRING
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TestV1_UTStarcom;
CREATE TABLE TestV1_UTStarcom (
      Account_Type1 STRING ,
      Account_Type2 STRING ,
      Func_Descrip STRING ,
      Location_Description STRING ,
      Date_ STRING,
      Income_State_Prop STRING ,
      Scenario_Type2 STRING ,
      Measure DOUBLE
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
drop table if exists TestV1_xy;
CREATE TABLE TestV1_xy (
      a STRING,
      x INT,
      y INT
      )
STORED AS orc tblproperties ("orc.compress"="SNAPPY");
insert overwrite table TestV1_Batters select * from testv1_raw.TestV1_Batters;
insert overwrite table TestV1_Calcs select * from testv1_raw.TestV1_Calcs;
insert overwrite table TestV1_DateBins select * from testv1_raw.TestV1_DateBins;
insert overwrite table TestV1_DateTime select * from testv1_raw.TestV1_DateTime;
insert overwrite table TestV1_Election select * from testv1_raw.TestV1_Election;
insert overwrite table TestV1_FischerIris select * from testv1_raw.TestV1_FischerIris;
insert overwrite table TestV1_Loan select * from testv1_raw.TestV1_Loan;
insert overwrite table TestV1_NumericBins select * from testv1_raw.TestV1_NumericBins;
insert overwrite table TestV1_REI select * from testv1_raw.TestV1_REI;
insert overwrite table TestV1_SeattleCrime select * from testv1_raw.TestV1_SeattleCrime;
insert overwrite table TestV1_Securities select * from testv1_raw.TestV1_Securities;
insert overwrite table TestV1_SpecialData select * from testv1_raw.TestV1_SpecialData;
insert overwrite table TestV1_Staples select * from testv1_raw.TestV1_Staples;
insert overwrite table TestV1_Starbucks select * from testv1_raw.TestV1_Starbucks;
insert overwrite table TestV1_UTStarcom select * from testv1_raw.TestV1_UTStarcom;
insert overwrite table TestV1_xy select * from testv1_raw.TestV1_xy;
