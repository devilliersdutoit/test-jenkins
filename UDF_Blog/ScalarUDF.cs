using Common.Logging;
using SphinxConnector;
using Microsoft.SqlServer.Server;
using System;
using System.Data.SqlTypes;
using System.Data;
using System.Security.Principal;
using System.Collections;



public partial class UserDefinedFunctions
{
    [SqlFunction(DataAccess = DataAccessKind.Read, FillRowMethodName = "FillRowCustomTable", TableDefinition = "prop_id decimal")]
    public static IEnumerable ScalarUDF(String inSearchString, String inMatchPercentage,String inIndex)
    {

        WindowsIdentity currentIdentity = null;
        WindowsImpersonationContext impersonatedIdentity = null;
        if (SqlContext.WindowsIdentity != null)
        {
            currentIdentity = SqlContext.WindowsIdentity;
            impersonatedIdentity = currentIdentity.Impersonate();
        }

            inMatchPercentage = "/0." + inMatchPercentage;

            string connectionString = "Data Source=localhost;Port=9306;pooling=true";
            double searchMatchRatio = 0.95;
            //string searchString = (string)SqlString.;
            string searchOptions = "OPTION ranker=sph04 ,comment='Origin: VS C#'";
            string indexName = "PropertyIndex";
            //string sphinxQLQuery = "SELECT * FROM PropertyIndex WHERE MATCH('\"" + inSearchString + "\"/" + inMatchRatio + "');";

            //string sphinxQLQuery = "SELECT * FROM PropertyIndex WHERE MATCH('\"" + inSearchString + "\"" + inMatchPercentage + "') OPTION ranker=sph04;";
            string sphinxQLQuery = "SELECT * FROM " + inIndex + " WHERE MATCH('\"" + inSearchString + "\"" + inMatchPercentage + "') OPTION ranker=sph04;";

            System.Data.DataTable dataTable = new System.Data.DataTable();

            try { 
                using (SphinxConnector.SphinxQL.SphinxQLConnection connection = new SphinxConnector.SphinxQL.SphinxQLConnection(connectionString))
                {
                    SphinxConnector.SphinxQL.SphinxQLCommand command = new SphinxConnector.SphinxQL.SphinxQLCommand(sphinxQLQuery, connection);
                    SphinxConnector.SphinxQL.SphinxQLDataAdapter dataAdapter = new SphinxConnector.SphinxQL.SphinxQLDataAdapter();
                    connection.Open();
                    dataAdapter.SelectCommand = command;
                
                    dataAdapter.Fill(dataTable);
                    dataAdapter.Dispose();
                    connection.Close();
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if (impersonatedIdentity != null)
                    impersonatedIdentity.Undo();
            }
            return dataTable.Rows;
    }


    public static void FillRowCustomTable(object resultObject, out Decimal prop_id)
    {
        DataRow resultRow = (DataRow)resultObject;
        prop_id = (Decimal)resultRow.ItemArray.GetValue(0);
    }

}
  