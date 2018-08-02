using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Drawing;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Windows.Forms;
using log4net;

namespace NetsSettlementAudit
{
    public partial class Form1 : Form
    {
        [assembly: log4net.Config.DOMConfigurator(ConfigFileExtension = "config", Watch = true)]
        static ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        Dictionary<string, string> carparklist = new Dictionary<string, string>();
        Dictionary<string, string> batchlist = new Dictionary<string, string>();
        Dictionary<string, string> batchIP = new Dictionary<string, string>();
        private string alarmTxt = "";
        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            Thread thr = new Thread(() => LetsRock());
            thr.Start();
        }

        private void InitCarparkList()
        {
            string constr = "Data Source=172.16.1.89;uid=secure;pwd=weishenme;database=carpark";
            string CommandText = @"select name,ip,batch from Whole where valid =1";
            //           string CommandText = @"select name,ip,batch from Whole where name='HG10'";
            DataSet ds = null;

            try
            {
                ds = SqlHelper.ExecuteDataset(constr, CommandType.Text, CommandText);
                foreach (DataRow ls in ds.Tables[0].Rows)
                {
                    carparklist.Add(ls[0].ToString(), ls[1].ToString());
                    batchlist.Add(ls[0].ToString(), ls[2].ToString());
                }
            }
            catch (SqlException sql)
            {
                log.Error($"Fail To Get Car Park List!{sql.ToString()}");
            }
            finally
            {
                try
                {
                    if (ds != null)
                        ds.Dispose();
                }
                catch (SqlException e)
                {
                    log.Error("Fail To Close Car Park List DataSet:" + e.ToString());
                }
            }

            string cmd_ = "SELECT* FROM [dbo].[ServerDetails]";
            string constr_ = "Data Source=172.16.1.89;uid=secure;pwd=weishenme;database=LTASettlementAudit";

            DataSet ds_ = null;

            try
            {
                ds_ = SqlHelper.ExecuteDataset(constr_, CommandType.Text, cmd_);
                foreach (DataRow ls in ds_.Tables[0].Rows)
                {
                    batchIP.Add(ls[0].ToString(), ls[1].ToString());
                }
            }
            catch (SqlException sql)
            {
                log.Error($"Fail To Get Car Park List!{sql.ToString()}");
            }
        }
        private void AlarmTxt(string str)
        {
            alarmTxt = alarmTxt + Environment.NewLine;
            alarmTxt = alarmTxt + str;
            alarmTxt = alarmTxt + Environment.NewLine;
        }
        private static List<string> GetEmail(string strkey)
        {
            List<string> list = new List<string>();
            foreach (string key in ConfigurationManager.AppSettings)
            {
                if (key.Contains(strkey))
                {
                    list.Add(ConfigurationManager.AppSettings[key]);
                }
            }
            return list;
        }
        private void SendEmail(string sub, string body, string address)
        {
            // Command line argument must the the SMTP host.
            SmtpClient client = new SmtpClient();
            client.Port = 587;
            client.Host = "smtp.gmail.com";
            client.EnableSsl = true;
            client.Timeout = 15000;
            client.DeliveryMethod = SmtpDeliveryMethod.Network;
            client.UseDefaultCredentials = false;
            client.Credentials = new NetworkCredential("seasonalarm@gmail.com", "wei3shen2me");
            MailMessage mm = new MailMessage("seasonalarm@gmail.com", address, sub, body);
            MailAddress copy1 = new MailAddress("jzhang@Secureparking.com.sg");
            // MailAddress copy2 = new MailAddress("leon@Secureparking.com.sg");
            // MailAddress copy3 = new MailAddress("schew@secureparking.com.sg");
            mm.CC.Add(copy1);    //CC email 
                                 // mm.CC.Add(copy2);
                                 // mm.CC.Add(copy3);
            mm.BodyEncoding = UTF8Encoding.UTF8;
            mm.DeliveryNotificationOptions = DeliveryNotificationOptions.OnFailure;

            List<string> lis = GetEmail("email");

            if (lis != null)
            {
                foreach (string str in lis)
                {
                    mm.To.Add(new MailAddress(str, "seasonalarm@gmail.com"));
                }
            }

            try
            {
                client.Send(mm);
                LogClass.WriteLog("Mail Sent! Success");
            }
            catch (Exception ex)
            {
                LogClass.WriteLog(ex.ToString());
            }
        }

        /*
         * 1,Read and collect from each pms and check if  settlement upload to nets server .SELECT * FROM [dbo].[settle_file_history] where settle_date BETWEEN '2018-07-31' and '2018-08-01' and station_id='ALL'
         * 2,Read logfile from nets server to verify upload status.
         * 3,Send email to alert.
         */
        private void LetsRock()
        {
            CleanDB();
            ReadDataFromPMS();
        }

        private void CleanDB()
        {
            string constr_server = "Data Source=172.16.1.89;uid=secure;pwd=weishenme;database=NetsSettlementAudit";
            string cmd = @"Delete [dbo].[settle_file_history] where Coll_Create_dt BETWEEN @start_time and @end_time";
            string start_time = DateTime.Now.ToString("yyyy-MM-dd 00:00:00");
            string end_time = DateTime.Now.AddDays(1).ToString("yyyy-MM-dd 00:00:00");

            SqlParameter[] para = new SqlParameter[]
             {
                    new SqlParameter("@start_time",start_time),
                    new SqlParameter("@end_time",end_time)
             };

            try
            {
                SqlHelper.ExecuteNonQuery(constr_server, CommandType.Text, cmd, para);
            }
            catch (SqlException e)
            {
                LogClass.WriteLog($"Read Server Settle_file_history Error {e.ToString()}");
                AlarmTxt($"Read Server Settle_file_history Error");
                return;
            }
        }

        private void ReadDataFromPMS()
        {
            string constr_server = "Data Source=172.16.1.89;uid=secure;pwd=weishenme;database=NetsSettlementAudit";
            string start_time = DateTime.Now.ToString("yyyy-MM-dd 00:00:00");
            string end_time = DateTime.Now.AddDays(1).ToString("yyyy-MM-dd 00:00:00");

            foreach (KeyValuePair<string, string> kv in carparklist)
            {
                SqlBulkCopy sbc = null;
                DataColumn CarparkColumn = null;
                DataColumn BatchColumn = null;
                string constr = $"Data Source={kv.Value};uid=sa;pwd=yzhh2007;database={kv.Key}";
                string cmd = @"SELECT * FROM [dbo].[settle_file_history] where settle_date BETWEEN @start_time and @end_time and station_id='ALL';";
                DataSet ds = null;
                SqlParameter[] para = new SqlParameter[]
                {
                    new SqlParameter("@start_time",start_time),
                    new SqlParameter("@end_time",end_time)
                };

                try
                {
                    ds = SqlHelper.ExecuteDataset(constr, CommandType.Text, cmd, para);
                    LogClass.WriteLog($"{kv.Key} Collected Data.");
                }
                catch (SqlException e)
                {
                    LogClass.WriteLog($"{kv.Key} read db fail,{e.ToString()}");
                    AlarmTxt($"{kv.Key} Fail To Collect Nets File Details,Please Check PMS Connection.");
                    continue;
                }

                if (ds == null)
                {
                    LogClass.WriteLog($"{kv.Key} dataset is null");
                    AlarmTxt($"{kv.Key} Dataset Is Null");
                    continue;
                }

                try
                {
                    if ((ds != null) && (ds.Tables[0].Rows.Count > 0))
                    {
                        CarparkColumn = new DataColumn();
                        CarparkColumn.DataType = System.Type.GetType("System.String");
                        CarparkColumn.ColumnName = "carparkID";
                        CarparkColumn.DefaultValue = kv.Key;

                        BatchColumn = new DataColumn();
                        BatchColumn.DataType = System.Type.GetType("System.String");
                        BatchColumn.ColumnName = "batch";
                        BatchColumn.DefaultValue = batchlist[kv.Key];

                        ds.Tables[0].Columns.Add(CarparkColumn);
                        ds.Tables[0].Columns.Add(BatchColumn);
                        sbc = new SqlBulkCopy(constr_server);
                        sbc.DestinationTableName = "settle_file_history";
                        sbc.WriteToServer(ds.Tables[0]);

                        foreach (DataRow dr in ds.Tables[0].Rows)
                        {
                            string settle_file = dr["settle_file"].ToString();
                            string send_flag = dr["send_flag"].ToString();
                            if (!send_flag.Equals("1"))
                            {
                                LogClass.WriteLog($"{kv.Key} Never Upload Settlement File: {settle_file}");
                                AlarmTxt($"{kv.Key} Never Upload Settlement File: {settle_file}");
                            }
                        }
                    }
                    else
                    {
                        LogClass.WriteLog($"{kv.Key} Have No Settlement Files Found.");
                        AlarmTxt($"{kv.Key} Have No Settlement Files Found.");
                    }
                }
                catch (Exception e)
                {
                    LogClass.WriteLog($"Found Error {e.ToString()}");
                    AlarmTxt($"{kv.Key} Found Unexpected Error.");
                }
            }
        }
    }
}
