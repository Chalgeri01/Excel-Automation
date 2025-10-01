
Attribute VB_Name = "SetupNewUdyamGS_Fixed"
Option Explicit

' === HOW TO USE ===
' 1) Open a blank workbook.
' 2) ALT+F11 -> File -> Import File... -> select this .bas file.
' 3) ALT+F8 -> run: Setup_New_UDYAM_GS_Report_Fixed
' 4) Save as New-UDYAM-GS Report.xlsb

Public Sub Setup_New_UDYAM_GS_Report_Fixed()
    Application.ScreenUpdating = False
    Application.DisplayAlerts = False
    Application.EnableEvents = False
    On Error GoTo CleanFail

    Dim wsSettings As Object ' Worksheet
    Dim wsStock As Object    ' Worksheet
    Dim wsToday As Object    ' Worksheet

    Set wsSettings = EnsureSheet("Settings")
    Set wsStock = EnsureSheet("UDYAM-GS STOCK REPORT")
    Set wsToday = EnsureSheet("UDYAM-GS Today_TransactionData")

    BuildTable1 wsSettings
    BuildTable2 wsSettings

    AddPQ_D
    AddPQ_DO
    AddPQ_O
    AddPQ_T

    LoadPQToSheet "O", wsStock, "A1"
    LoadPQToSheet "T", wsToday, "A1"

    Dim cn As Object ' WorkbookConnection
    For Each cn In ThisWorkbook.Connections
        On Error Resume Next
        If HasMember(cn, "OLEDBConnection") Then
            cn.OLEDBConnection.BackgroundQuery = False
        End If
        If HasMember(cn, "ODBCConnection") Then
            cn.ODBCConnection.BackgroundQuery = False
        End If
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

Private Function EnsureSheet(ByVal name As String) As Object ' Worksheet
    Dim ws As Object ' Worksheet
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

Private Sub BuildTable1(ByVal ws As Object) ' Worksheet
    ' Table1: DoC Code | Doc Name | Doc type
    With ws
        .Range("A1").Value = "DoC Code"
        .Range("B1").Value = "Doc Name"
        .Range("C1").Value = "Doc type"
        .Range("A2").Value = 1001
        .Range("B2").Value = "Sample Doc"
        .Range("C2").Value = "TypeA"

        Dim lo As Object ' ListObject
        On Error Resume Next
        Set lo = .ListObjects("Table1")
        On Error GoTo 0
        If lo Is Nothing Then
            Set lo = .ListObjects.Add(SourceType:=xlSrcRange, Source:=.Range("A1").CurrentRegion, XlListObjectHasHeaders:=xlYes)
            lo.Name = "Table1"
        Else
            lo.Resize .Range("A1").CurrentRegion
        End If
    End With
End Sub

Private Sub BuildTable2(ByVal ws As Object) ' Worksheet
    With ws
        .Range("E1").Value = "Date"
        .Range("E2").Value = Date

        Dim lo As Object ' ListObject
        On Error Resume Next
        Set lo = .ListObjects("Table2")
        On Error GoTo 0
        If lo Is Nothing Then
            Set lo = .ListObjects.Add(SourceType:=xlSrcRange, Source:=.Range("E1").CurrentRegion, XlListObjectHasHeaders:=xlYes)
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
    Dim q As Object ' Query
    On Error Resume Next
    Set q = ThisWorkbook.Queries(qName)
    On Error GoTo 0
    If q Is Nothing Then
        ThisWorkbook.Queries.Add Name:=qName, Formula:=mCode
    Else
        q.Formula = mCode
    End If
End Sub

Private Sub LoadPQToSheet(ByVal qName As String, ByVal ws As Object, ByVal topLeft As String)
    Dim connName As String
    connName = "Query - " & qName

    On Error Resume Next
    ThisWorkbook.Connections(connName).Delete
    On Error GoTo 0

    Dim c As Object ' WorkbookConnection
    ' Use Connections.Add (not Add2) for compatibility
    Set c = ThisWorkbook.Connections.Add( _
        Name:=connName, _
        Description:="", _
        ConnectionString:="OLEDB;Provider=Microsoft.Mashup.OleDb.1;Data Source=$Workbook$;Location=" & qName & ";Extended Properties=""""" _
    )

    Dim lo As Object ' ListObject
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

Private Function HasMember(ByVal obj As Object, ByVal memberName As String) As Boolean
    On Error Resume Next
    Dim tmp As Object
    Set tmp = CallByName(obj, memberName, VbGet)
    HasMember = (Err.Number = 0)
    Err.Clear
    On Error GoTo 0
End Function
