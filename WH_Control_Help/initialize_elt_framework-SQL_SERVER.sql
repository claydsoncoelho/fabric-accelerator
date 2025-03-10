SELECT 
    tab.Table_Schema as Schema_Name, 
    Tab.Table_Name,
    STUFF(
        (SELECT '|' + Col.Column_Name
         FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col
         WHERE Col.Constraint_Name = Tab.Constraint_Name
           AND Col.Table_Name = Tab.Table_Name
         AND Tab.Table_Name IN ('F1ASR_REG_BOOK', 'F1ASR_ASS_BK_TRAN', 'F1ASR_ASSET_BOOK')
         FOR XML PATH('')), 
        1, 1, '') AS [primary_key]
FROM 
    INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab
WHERE 
    Tab.Constraint_Type = 'PRIMARY KEY'
    AND Tab.Table_Name IN ('F1ASR_REG_BOOK', 'F1ASR_ASS_BK_TRAN', 'F1ASR_ASSET_BOOK')
GROUP BY 
    tab.Table_Schema,
    Tab.Table_Name,
    Tab.Constraint_Name;



SELECT 
    'SELECT ''' + 
    tab.Table_Schema + 
    ''' AS Schema_Name, ''' +  
    Tab.Table_Name +
    ''' AS Table_Name, ''' +
    STUFF(
        (SELECT '|' + Col.Column_Name
         FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col
         WHERE Col.Constraint_Name = Tab.Constraint_Name
           AND Col.Table_Name = Tab.Table_Name
         AND Tab.Table_Name IN ('F1ASR_REG_BOOK', 'F1ASR_ASS_BK_TRAN', 'F1ASR_ASSET_BOOK')
         FOR XML PATH('')), 
        1, 1, '') + 
    ''' AS primary_key UNION '
FROM 
    INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab
WHERE 
    Tab.Constraint_Type = 'PRIMARY KEY'
    AND Tab.Table_Name IN ('F1ASR_REG_BOOK', 'F1ASR_ASS_BK_TRAN', 'F1ASR_ASSET_BOOK')
GROUP BY 
    tab.Table_Schema,
    Tab.Table_Name,
    Tab.Constraint_Name;
