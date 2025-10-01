
Attribute VB_Name = "SetupNewUdyamGS"
Option Explicit

' === HOW TO USE ===
' 1) Open a blank workbook.
' 2) Press ALT+F11 (VBA editor) -> File -> Import File... -> select this .bas file.
' 3) Back in Excel, press ALT+F8 -> run: Setup_New_UDYAM_GS_Report
' 4) Save the workbook as New-UDYAM-GS Report.xlsb
'
' Requirements:
' - Excel with Power Query (Get & Transform)
' - ODBC DSN named: KAPL_GOLIVE (same bitness as Excel)
'
' This will:
' - Create sheets: Settings, UDYAM-GS STOCK REPORT, UDYAM-GS Today_TransactionData
' - Create Table1 (Doc lookup) and Table2 (Date list)
' - Add Power Queries D, DO, O, T
' - Load O to "UDYAM-GS STOCK REPORT", T to "UDYAM-GS Today_TransactionData"
' - Set connections to not use background refresh

Public Sub Setup_New_UDYAM_GS_Report()
    Application.ScreenUpdating = False
    Application.DisplayAlerts = False
    Application.EnableEvents = False
    On Error GoTo CleanFail

    Dim wsSettings As Worksheet
    Dim wsStock As Worksheet
    Dim wsToday As Worksheet

    ' Create or get sheets
    Set wsSettings = EnsureSheet("Settings")
    Set wsStock = EnsureSheet("UDYAM-GS STOCK REPORT")
    Set wsToday = EnsureSheet("UDYAM-GS Today_TransactionData")

    ' Build Table1 and Table2 on Settings
    BuildTable1 wsSettings           ' Doc lookup
    BuildTable2 wsSettings           ' Date list

    ' Add Power Query queries
    AddPQ_D
    AddPQ_DO
    AddPQ_O
    AddPQ_T

    ' Load queries O and T to sheets as tables
    LoadPQToSheet "O", wsStock, "A1"
    LoadPQToSheet "T", wsToday, "A1"

    ' Turn off background refresh on PQ connections
    Dim cn As WorkbookConnection
    For Each cn In ThisWorkbook.Connections
        On Error Resume Next
        If cn.Type = xlConnectionTypeOLEDB Then cn.OLEDBConnection.BackgroundQuery = False
        If cn.Type = xlConnectionTypeODBC Then cn.ODBCConnection.BackgroundQuery = False
        On Error GoTo 0
    Next cn

    MsgBox "Setup complete. Save this workbook as 'New-UDYAM-GS Report.xlsb'."

CleanExit:
    Application.EnableEvents = True
    Application.DisplayAlerts = True
    Application.ScreenUpdating = True
    Exit Sub

CleanFail:
    MsgBox "Error " & Err.Number & ": " & Err.Description, vbCritical
    Resume CleanExit
End Sub

Private Function EnsureSheet(ByVal name As String) As Worksheet
    Dim ws As Worksheet
    On Error Resume Next
    Set ws = ThisWorkbook.Worksheets(name)
    On Error GoTo 0
    If ws Is Nothing Then
        Set ws = ThisWorkbook.Worksheets.Add(After:=ThisWorkbook.Worksheets(ThisWorkbook.Worksheets.Count))
        ws.Name = name
    Else
        ws.Cells.Clear
    End If
    Set EnsureSheet = ws
End Function

Private Sub BuildTable1(ByVal ws As Worksheet)
    ' Table1: DoC Code | Doc Name | Doc type
    With ws
        .Range("A1").Value = "DoC Code"
        .Range("B1").Value = "Doc Name"
        .Range("C1").Value = "Doc type"
        ' sample row (optional)
        .Range("A2").Value = 1001
        .Range("B2").Value = "Sample Doc"
        .Range("C2").Value = "TypeA"
        ' Create table
        Dim lo As ListObject
        On Error Resume Next
        Set lo = .ListObjects("Table1")
        On Error GoTo 0
        If lo Is Nothing Then
            Set lo = .ListObjects.Add( _
                SourceType:=xlSrcRange, _
                Source:=.Range("A1").CurrentRegion, _
                XlListObjectHasHeaders:=xlYes)
            lo.Name = "Table1"
        Else
            lo.Resize .Range("A1").CurrentRegion
        End If
    End With
End Sub

Private Sub BuildTable2(ByVal ws As Worksheet)
    ' Table2: Date
    With ws
        .Range("E1").Value = "Date"
        .Range("E2").Value = Date
        ' Create table
        Dim lo As ListObject
        On Error Resume Next
        Set lo = .ListObjects("Table2")
        On Error GoTo 0
        If lo Is Nothing Then
            Set lo = .ListObjects.Add( _
                SourceType:=xlSrcRange, _
                Source:=.Range("E1").CurrentRegion, _
                XlListObjectHasHeaders:=xlYes)
            lo.Name = "Table2"
        Else
            lo.Resize .Range("E1").CurrentRegion
        End If
    End With
End Sub

Private Sub AddPQ_D()
    Dim m As String
    m = _
    "let" & vbCrLf & _
    "    Source = Excel.CurrentWorkbook(){[Name=""Table2""]}[Content]," & vbCrLf & _
    "    #""Changed Type"" = Table.TransformColumnTypes(Source,{{""Date"", type date}})," & vbCrLf & _
    "    #""Added Custom"" = Table.AddColumn(#""Changed Type"", ""Data"", each ""Today"")," & vbCrLf & _
    "    #""Changed Type1"" = Table.TransformColumnTypes(#""Added Custom"",{{""Data"", type text}})" & vbCrLf & _
    "in" & vbCrLf & _
    "    #""Changed Type1"""
    AddOrReplaceQuery "D", m
End Sub

Private Sub AddPQ_DO()
    Dim m As String
    m = _
    "let" & vbCrLf & _
    "    Source = Excel.CurrentWorkbook(){[Name=""Table1""]}[Content]," & vbCrLf & _
    "    #""Changed Type"" = Table.TransformColumnTypes(Source,{{""DoC Code"", Int64.Type}, {""Doc Name"", type text}, {""Doc type"", type text}})" & vbCrLf & _
    "in" & vbCrLf & _
    "    #""Changed Type"""
    AddOrReplaceQuery "DO", m
End Sub

Private Sub AddPQ_O()
    Dim m As String
    m = _
    "let" & vbCrLf & _
    "    Source = Odbc.DataSource(""dsn=KAPL_GOLIVE"", [HierarchicalNavigation=true])," & vbCrLf & _
    "    KAPL_GOLIVE_Schema = Source{[Name=""KAPL_GOLIVE"",Kind=""Schema""]}[Data]," & vbCrLf & _
    "    WHS_View = KAPL_GOLIVE_Schema{[Name=""SURESH_UDYAM_WHS_Stock_REPORT"",Kind=""View""]}[Data]," & vbCrLf & _
    "    #""Changed Type"" = Table.TransformColumnTypes(WHS_View,{{""Posting Date"", type date}})," & vbCrLf & _
    "    #""Added Custom"" = Table.AddColumn(#""Changed Type"", ""Stock"", each [IN Quantity]-[Out Quantity])," & vbCrLf & _
    "    #""Merged Queries"" = Table.NestedJoin(#""Added Custom"", {""Posting Date""}, D, {""Date""}, ""D"", JoinKind.LeftOuter)," & vbCrLf & _
    "    #""Expanded D"" = Table.ExpandTableColumn(#""Merged Queries"", ""D"", {""Data""}, {""Data""})," & vbCrLf & _
    "    #""Replaced Value"" = Table.ReplaceValue(#""Expanded D"", null, ""Opening"", Replacer.ReplaceValue, {""Data""})," & vbCrLf & _
    "    #""Merged Queries1"" = Table.NestedJoin(#""Replaced Value"", {""Doc Code""}, DO, {""DoC Code""}, ""DO"", JoinKind.LeftOuter)," & vbCrLf & _
    "    #""Expanded DO"" = Table.ExpandTableColumn(#""Merged Queries1"", ""DO"", {""Doc Name"", ""Doc type""}, {""Doc Name"", ""Doc type""})," & vbCrLf & _
    "    #""Filtered Rows"" = Table.SelectRows(#""Expanded DO"", each [Warehouse Code] = ""UDYAM-GS"")" & vbCrLf & _
    "in" & vbCrLf & _
    "    #""Filtered Rows"""
    AddOrReplaceQuery "O", m
End Sub

Private Sub AddPQ_T()
    Dim m As String
    m = _
    "let" & vbCrLf & _
    "    Source = Odbc.DataSource(""dsn=KAPL_GOLIVE"", [HierarchicalNavigation=true])," & vbCrLf & _
    "    KAPL_GOLIVE_Schema = Source{[Name=""KAPL_GOLIVE"",Kind=""Schema""]}[Data]," & vbCrLf & _
    "    CNC_View = KAPL_GOLIVE_Schema{[Name=""SURESH_UDYAM-CNC_WHS_REPORT"",Kind=""View""]}[Data]," & vbCrLf & _
    "    #""Changed Type"" = Table.TransformColumnTypes(CNC_View,{{""Posting Date"", type date}})," & vbCrLf & _
    "    #""Added Custom"" = Table.AddColumn(#""Changed Type"", ""Stock"", each [IN Quantity]-[Out Quantity])," & vbCrLf & _
    "    #""Merged Queries"" = Table.NestedJoin(#""Added Custom"", {""Posting Date""}, D, {""Date""}, ""D"", JoinKind.Inner)," & vbCrLf & _
    "    #""Expanded D"" = Table.ExpandTableColumn(#""Merged Queries"", ""D"", {""Data""}, {""Data""})," & vbCrLf & _
    "    #""Replaced Value"" = Table.ReplaceValue(#""Expanded D"", null, ""Opening"", Replacer.ReplaceValue, {""Data""})," & vbCrLf & _
    "    #""Merged Queries1"" = Table.NestedJoin(#""Replaced Value"", {""Doc Code""}, DO, {""DoC Code""}, ""DO"", JoinKind.LeftOuter)," & vbCrLf & _
    "    #""Expanded DO"" = Table.ExpandTableColumn(#""Merged Queries1"", ""DO"", {""Doc Name"", ""Doc type""}, {""Doc Name"", ""Doc type""})" & vbCrLf & _
    "in" & vbCrLf & _
    "    #""Expanded DO"""
    AddOrReplaceQuery "T", m
End Sub

Private Sub AddOrReplaceQuery(ByVal qName As String, ByVal mCode As String)
    Dim q As Query
    On Error Resume Next
    Set q = ThisWorkbook.Queries(qName)
    On Error GoTo 0
    If q Is Nothing Then
        ThisWorkbook.Queries.Add Name:=qName, Formula:=mCode
    Else
        q.Formula = mCode
    End If
End Sub

Private Sub LoadPQToSheet(ByVal qName As String, ByVal ws As Worksheet, ByVal topLeft As String)
    ' Creates a Mashup (Power Query) connection and loads it to a table on the given sheet.
    Dim connName As String
    connName = "Query - " & qName

    ' Remove existing connection/table if present
    On Error Resume Next
    ThisWorkbook.Connections(connName).Delete
    On Error GoTo 0

    ' Create the Mashup (Power Query) connection pointing at this query
    ' This magic connection string tells Excel to use the embedded PQ "qName"
    Dim c As WorkbookConnection
    Set c = ThisWorkbook.Connections.Add2( _
        Name:=connName, _
        Description:="", _
        ConnectionString:="OLEDB;Provider=Microsoft.Mashup.OleDb.1;Data Source=$Workbook$;Location=" & qName & ";Extended Properties=""""" _
    )

    ' Now create a ListObject bound to that connection at the destination
    Dim lo As ListObject
    Set lo = ws.ListObjects.Add(SourceType:=0, Source:=Array( _
        "OLEDB;Provider=Microsoft.Mashup.OleDb.1;Data Source=$Workbook$;Location=" & qName & ";Extended Properties=""""" _
        ), Destination:=ws.Range(topLeft))

    lo.Name = "tbl_" & qName

    With lo.QueryTable
        .WorkbookConnection = c
        .AdjustColumnWidth = True
        .RefreshStyle = xlInsertDeleteCells
        .Refresh BackgroundQuery:=False
    End With
End Sub
