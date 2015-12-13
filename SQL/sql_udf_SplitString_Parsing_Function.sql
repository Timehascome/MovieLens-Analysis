USE [MovieLens]
GO

/****** Object:  UserDefinedFunction [dbo].[udf_SplitString]    Script Date: 11/28/2015 11:07:26 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[udf_SplitString] (@stringIn VARCHAR(MAX), @delimiter VARCHAR(MAX))
RETURNS TABLE
AS
RETURN
	WITH parseString AS(
		SELECT CAST(0 AS BIGINT) as index1, CHARINDEX(@delimiter, @stringIn) as index2
		UNION ALL
		SELECT index2+1, CHARINDEX(@delimiter, @stringIn, index2+1)
		  FROM parseString
		 WHERE index2 > 0
	)
	SELECT SUBSTRING(@stringIn, index1, COALESCE(NULLIF(index2,0), LEN(@stringIn) + 1) - index1) as value
	  FROM parseString;
GO


