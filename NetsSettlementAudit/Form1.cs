using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Drawing;
using System.IO;
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
        Dictionary<string, string> BatchNetsServer = new Dictionary<string, string>();
        Dictionary<string, string> SettleFileName = new Dictionary<string, string>();
        Dictionary<string, string> SettleFileBatch = new Dictionary<string, string>();
        private string alarmTxt = "";
        bool Ismanual = false;

        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {

            string constr_batch = "Data Source=172.16.1.89;uid=secure;pwd=weishenme;database=carpark";
            string CommandText_batch = @"SELECT name,ip from Whole WHERE valid=1;
                                         SELECT batch from BatchTable;                      ";
            DataSet ds_batch = null;
            try
            {
                ds_batch = SqlHelper.ExecuteDataset(constr_batch, CommandType.Text, CommandText_batch);
                comboBox1.DataSource = ds_batch.Tables[0];
                comboBox1.DisplayMember = "name";
                comboBox1.ValueMember = "ip";
                comboBox2.DataSource = ds_batch.Tables[1];
                comboBox2.DisplayMember = "batch";
            }
            catch (SqlException)
            {
                LogClass.WriteLog("Fail To Get Car Park List!");
            }

            if (!GetValue("manual").Equals("true"))
            {
                comboBox2.Text = "All";
                InitCarparkList();
                button1.Enabled = false;
                Thread thr = new Thread(() => LetsRock());
                thr.Start();
            }
            else
            {
                Ismanual = true;
            }

            //string carpark = "CENDEXDB";
            //string ip = "192.168.12.4";
            //string start_date = DateTime.Now.AddDays(-1).ToString("yyyyMMdd");
            //string end_date = DateTime.Now.AddDays(-1).ToString("yyyyMMdd");
            //StoredProcedureSpProcessConsolidated(carpark, ip, start_date, end_date);
            //StoredProcedureSjAddCashCardCollection(carpark, ip, start_date, end_date);
            //StoredProcedureSjAddLTACollection(carpark, ip, start_date, end_date);
            //Application.Exit();
        }
        private void button1_Click(object sender, EventArgs e)
        {
            InitCarparkList();
            Thread thr = new Thread(() => LetsRock());
            thr.Start();
        }
        private void InitCarparkList()
        {
            SettleFileName.Clear();
            carparklist.Clear();
            batchlist.Clear();
            BatchNetsServer.Clear();

            string carpark = comboBox1.Text.Trim().ToString();
            string batchname = comboBox2.Text.Trim().ToString();
            //LogClass.WriteLog($"carpark = {carpark},batch={batchname}");
            string CommandText = null;
            if (batchname.Equals("Null") || batchname.Equals(""))
            {
                CommandText = $"select name,ip,batch from Whole where valid=1 and name='{carpark}'";
            }
            else if (batchname.Equals("All"))
            {
                CommandText = $"select name,ip,batch from Whole where valid=1";
            }
            else
            {
                CommandText = $"select name,ip,batch from Whole where valid=1 and batch='{batchname}'";
            }
            string constr = "Data Source=172.16.1.89;uid=secure;pwd=weishenme;database=carpark";

            //LogClass.WriteLog($"cmd={CommandText}");
            //string CommandText = @"select name,ip,batch from Whole where valid=1";
            //string CommandText = @"select name,ip,batch from Whole where valid=1 and batch='JE'";
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
            string constr_ = "Data Source=172.16.1.89;uid=secure;pwd=weishenme;database=NetsSettlementAudit";

            DataSet ds_ = null;

            try
            {
                ds_ = SqlHelper.ExecuteDataset(constr_, CommandType.Text, cmd_);
                foreach (DataRow ls in ds_.Tables[0].Rows)
                {
                    BatchNetsServer.Add(ls[0].ToString(), ls[1].ToString());
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
        private static string GetValue(string strkey)
        {

            foreach (string key in ConfigurationManager.AppSettings)
            {
                if (key.Contains(strkey))
                {
                    return ConfigurationManager.AppSettings[key];
                }
            }
            return "";

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
            //MailAddress copy1 = new MailAddress("jzhang@Secureparking.com.sg");
            // MailAddress copy2 = new MailAddress("leon@Secureparking.com.sg");
            // MailAddress copy3 = new MailAddress("schew@secureparking.com.sg");
            //mm.CC.Add(copy1);    //CC email 
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
            //InitCarparkList();
            //CleanDB();
            alarmTxt = "";
            AlarmTxt($"===================== Check PMS ========================");
            ReadDataFromPMS();
            AlarmTxt($"================== Check Nets Server =====================");
            CheckSettlementFilesAtServer();
            if (!Ismanual)
            {
                SendEmail("Daily check for Nets Settlement", alarmTxt, "jzhang@secureparking.com.sg");
                Application.Exit();
            }
            else
            {
                MessageBox.Show(alarmTxt);
            }
            //SendEmail("Daily check for Nets Settlement", alarmTxt, "jzhang@secureparking.com.sg");
            //Application.Exit();
        }
        private void CleanDB()
        {
            string constr_server = "Data Source=172.16.1.89;uid=secure;pwd=weishenme;database=NetsSettlementAudit";
            string cmd = @"Delete [dbo].[settle_file_history] where settle_date BETWEEN @start_time and @end_time";
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

        //Process Nets and LTA Consolidated Report
        private bool StoredProcedureSpProcessConsolidated(string capark, string ip, string DateFrom, string DateTo)
        {
            SqlConnection connection = new SqlConnection($"Initial Catalog={capark};data source={ip},1433;user id=sa; password=yzhh2007; Network Library=DBMSSOCN;");
            try
            {
                SqlCommand command = new SqlCommand
                {
                    CommandText = "sp_processconsolidated",
                    CommandType = CommandType.StoredProcedure
                };
                SqlParameter parameter = command.Parameters.Add("@datefrom", SqlDbType.VarChar, 15);
                SqlParameter parameter2 = command.Parameters.Add("@dateto", SqlDbType.VarChar, 15);
                SqlParameter parameter3 = command.Parameters.Add("@option", SqlDbType.SmallInt, 15);
                parameter.Value = DateFrom;
                parameter2.Value = DateTo;
                parameter3.Value = 2;
                command.Connection = connection;
                if (connection.State == ConnectionState.Closed)
                {
                    connection.Open();
                }
                command.ExecuteNonQuery();
                LogClass.WriteLog("Process consolidated report ok.");
                command = null;
                connection.Close();
                return true;
            }
            catch (Exception e1)
            {
                LogClass.WriteLog($"find error to process consolidated report,{e1.ToString()}");
                return false;
            }
        }


        //Add CashCarsh Report.
        private bool StoredProcedureSjAddCashCardCollection(string capark, string ip, string DateFrom, string DateTo)
        {
            SqlConnection connection = new SqlConnection($"Initial Catalog={capark};data source={ip},1433;user id=sa; password=yzhh2007; Network Library=DBMSSOCN;");
            try
            {
                SqlCommand command = new SqlCommand
                {
                    CommandText = "sj_AddCashcardCollection",
                    CommandType = CommandType.StoredProcedure
                };
                SqlParameter parameter = command.Parameters.Add("@datefrom", SqlDbType.VarChar, 15);
                SqlParameter parameter2 = command.Parameters.Add("@dateto", SqlDbType.VarChar, 15);
                SqlParameter parameter3 = command.Parameters.Add("@type", SqlDbType.SmallInt, 15);
                parameter.Value = DateFrom;
                parameter2.Value = DateTo;
                parameter3.Value = 10;
                command.Connection = connection;
                if (connection.State == ConnectionState.Closed)
                {
                    connection.Open();
                }
                command.ExecuteNonQuery();
                LogClass.WriteLog("Process cashcard report ok.");
                command = null;
                connection.Close();
                return true;
            }
            catch (Exception e1)
            {
                LogClass.WriteLog($"find error to process cashcard report,{e1.ToString()}");
                return false;
            }
        }

        //Add LTA Report.
        private bool StoredProcedureSjAddLTACollection(string capark, string ip, string DateFrom, string DateTo)
        {
            SqlConnection connection = new SqlConnection($"Initial Catalog={capark};data source={ip},1433;user id=sa; password=yzhh2007; Network Library=DBMSSOCN;");
            try
            {
                SqlCommand command = new SqlCommand
                {
                    CommandText = "sj_AddLTACollection",
                    CommandType = CommandType.StoredProcedure
                };
                SqlParameter parameter = command.Parameters.Add("@datefrom", SqlDbType.VarChar, 15);
                SqlParameter parameter2 = command.Parameters.Add("@dateto", SqlDbType.VarChar, 15);
                SqlParameter parameter3 = command.Parameters.Add("@type", SqlDbType.SmallInt, 15);
                parameter.Value = DateFrom;
                parameter2.Value = DateTo;
                parameter3.Value = 10;
                command.Connection = connection;
                if (connection.State == ConnectionState.Closed)
                {
                    connection.Open();
                }
                command.ExecuteNonQuery();
                LogClass.WriteLog("Process LTA report ok.");
                command = null;
                connection.Close();
                return true;
            }
            catch (Exception e1)
            {
                LogClass.WriteLog($"find error to process LTA report,{e1.ToString()}");
                return false;
            }
        }

        private void ReadDataFromPMS()
        {
            SettleFileBatch.Clear();
            string constr_server = "Data Source=172.16.1.89;uid=secure;pwd=weishenme;database=NetsSettlementAudit";
            DateTime dt = DateTime.Now;
            string start_time = dt.ToString("yyyy-MM-dd 00:00:00");
            string end_time = dt.AddDays(1).ToString("yyyy-MM-dd 00:00:00");
            string collection_date = dt.AddDays(-1).ToString("yyyy-MM-dd 00:00:00");
            string P_date = DateTime.Now.AddDays(-1).ToString("yyyyMMdd");
            foreach (KeyValuePair<string, string> kv in carparklist)
            {
                LogClass.WriteLog($"=========={kv.Key}==========");
                if (!((StoredProcedureSpProcessConsolidated(kv.Key, kv.Value, P_date, P_date)) && (StoredProcedureSjAddCashCardCollection(kv.Key, kv.Value, P_date, P_date)) && (StoredProcedureSjAddLTACollection(kv.Key, kv.Value, P_date, P_date))))
                {
                    continue;
                }
                string constr = $"Data Source={kv.Value};uid=sa;pwd=yzhh2007;database={kv.Key}";
                string cmd = @"SELECT * FROM [dbo].[settle_file_history] where settle_date BETWEEN @start_time and @end_time and station_id='ALL';
                               SELECT * FROM [dbo].[daily_cashcard_collection] where trans_date=@collection_date;
                               SELECT value FROM [dbo].[parameter_mst] where name='CHUIP';
                               SELECT * FROM [dbo].[daily_lta_collection] where trans_date=@collection_date;";
                DataSet ds = null;

                string[] str = kv.Value.Split('.');
                string ipgroup = $"{str[0]}.{str[1]}.{str[2]}";

                SqlParameter[] para = new SqlParameter[]
                {
                    new SqlParameter("@start_time",start_time),
                    new SqlParameter("@end_time",end_time),
                    new SqlParameter("@collection_date",collection_date)
                };

                try
                {
                    ds = SqlHelper.ExecuteDataset(constr, CommandType.Text, cmd, para);
                    LogClass.WriteLog($"{kv.Key} Collected Nets Data.");
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

                    //Update LTA settlement amount.
                    if ((ds != null) && (ds.Tables[3].Rows.Count > 0))
                    {

                        try
                        {
                            double conso_amt = Convert.ToDouble(ds.Tables[3].Rows[0]["conso_amt"].ToString());
                            double settle_amt = Convert.ToDouble(ds.Tables[3].Rows[0]["settle_amt"].ToString());
                            //Update CPT amt;
                            string cmd_update_cpt = @"IF EXISTS (SELECT * FROM [dbo].[DailyRevenueSummary] where Trans_date=@Trans_date and carparkID=@carparkID) 
                                                            BEGIN
                                                               --update
                                                                Update DailyRevenueSummary set LTA_conso=@LTA_conso,LTA_settle=@LTA_settle,update_date=getdate(),ipgroup=@ipgroup where Trans_date=@Trans_date and carparkID=@carparkID
                                                            END
                                                            ELSE
                                                            BEGIN
                                                               -- insert
                                                               Insert INTO DailyRevenueSummary(carparkID,batch,LTA_conso,LTA_settle,Trans_date,update_date,ipgroup)VALUES(@carparkID,@batch,@LTA_conso,@LTA_settle,@Trans_date,getdate(),@ipgroup)
                                                            END";

                            SqlParameter[] para_LTA = new SqlParameter[]
                               {
                                        new SqlParameter("@carparkID",kv.Key),
                                        new SqlParameter("@batch",batchlist[kv.Key]),
                                        new SqlParameter("@LTA_conso",conso_amt),
                                        new SqlParameter("@LTA_settle",settle_amt),
                                        new SqlParameter("@ipgroup",ipgroup),
                                        new SqlParameter("@Trans_date",collection_date)
                               };

                            try
                            {
                                SqlHelper.ExecuteNonQuery(constr_server, CommandType.Text, cmd_update_cpt, para_LTA);
                                LogClass.WriteLog("Insert Data to Server Ok.");
                            }
                            catch (SqlException sqle)
                            {
                                LogClass.WriteLog($"Found Error When Insert To Server. {sqle.ToString()}");
                                //AlarmTxt($"{kv.Key} Have No CPT File Amount={conso_amt.ToString()} Generated,Please Check.");
                            }


                        }
                        catch (SqlException sqle)
                        {



                        }
                    }
                    else
                    {
                        LogClass.WriteLog($"No Lta data can collect.");
                    }




                    //Collect Settlement File Name.
                    if ((ds != null) && (ds.Tables[0].Rows.Count > 0))
                    {
                        foreach (DataRow dr_3 in ds.Tables[0].Rows)
                        {
                            string settle_file_path = dr_3["settle_file"].ToString();  //C:\Carpark\Settle\Pack\NETS\2018\T4P080100
                            string settle_sys = dr_3["settle_sys"].ToString();    // 0 CPT 1 CHU

                            string[] array = settle_file_path.Split('\\');
                            string file_name = array[6];
                            switch (settle_sys)
                            {
                                case "0":
                                    SettleFileName.Add(file_name, kv.Key + "_CPT");
                                    break;
                                case "1":
                                    SettleFileName.Add(file_name, kv.Key + "_CHU");
                                    break;
                            }
                            SettleFileBatch.Add(file_name, batchlist[kv.Key]);
                        }

                    }



                    //Check by daily_cashcard_collection.
                    if ((ds != null) && (ds.Tables[1].Rows.Count > 0))
                    {
                        string value = ds.Tables[2].Rows[0]["value"].ToString();
                        LogClass.WriteLog($"CHUIP:{value}");
                        bool isCHUIPSame = false;
                        if (value.Equals(kv.Key))
                        {
                            isCHUIPSame = true;
                        }
                        foreach (DataRow dr in ds.Tables[1].Rows)
                        {

                            string trans_type = dr["trans_type"].ToString();
                            double conso_amt = Convert.ToDouble(dr["conso_amt"].ToString());
                            double settle_amt = Convert.ToDouble(dr["settle_amt"].ToString());
                            string trans_date = dr["trans_date"].ToString();


                            bool flag_cpt = false;
                            bool flag_chu = false;
                            if (trans_type.Equals("1") && (conso_amt > 0))    //1 CPT Consolidated.
                            {

                                //Update CPT amt;
                                string cmd_update_cpt = @"IF EXISTS (SELECT * FROM [dbo].[DailyRevenueSummary] where Trans_date=@Trans_date and carparkID=@carparkID) 
                                                            BEGIN
                                                               --update
                                                                Update DailyRevenueSummary set cpt_conso=@cpt_conso,CPT_settle=@CPT_settle,update_date=getdate(),ipgroup=@ipgroup where Trans_date=@Trans_date and carparkID=@carparkID
                                                            END
                                                            ELSE
                                                            BEGIN
                                                               -- insert
                                                               Insert INTO DailyRevenueSummary(carparkID,batch,CPT_conso,CPT_settle,Trans_date,update_date,ipgroup)VALUES(@carparkID,@batch,@CPT_conso,@CPT_settle,@Trans_date,getdate(),@ipgroup)
                                                            END";

                                SqlParameter[] para_cpt = new SqlParameter[]
                                {
                                        new SqlParameter("@carparkID",kv.Key),
                                        new SqlParameter("@batch",batchlist[kv.Key]),
                                        new SqlParameter("@cpt_conso",conso_amt),
                                        new SqlParameter("@CPT_settle",settle_amt),
                                        new SqlParameter("@ipgroup",ipgroup),
                                        new SqlParameter("@Trans_date",trans_date)
                                };

                                try
                                {
                                    SqlHelper.ExecuteNonQuery(constr_server, CommandType.Text, cmd_update_cpt, para_cpt);
                                    LogClass.WriteLog("Insert Data to Server Ok.");
                                }
                                catch (SqlException sqle)
                                {
                                    LogClass.WriteLog($"Found Error When Insert To Server. {sqle.ToString()}");
                                    //AlarmTxt($"{kv.Key} Have No CPT File Amount={conso_amt.ToString()} Generated,Please Check.");
                                }




                                if (ds.Tables[0].Rows.Count > 0)
                                {
                                    foreach (DataRow dr_2 in ds.Tables[0].Rows)
                                    {
                                        string settle_sys = dr_2["settle_sys"].ToString();
                                        string settle_file = dr_2["settle_file"].ToString();
                                        string send_flag = dr_2["send_flag"].ToString();
                                        double total_amt = Convert.ToDouble(dr_2["total_amt"].ToString());
                                        if (settle_sys.Equals("0"))   //0 CPT 1 CHU Settled.
                                        {
                                            flag_cpt = true;
                                            if (send_flag.Equals("1"))
                                            {
                                                //CPT Upload ok.
                                                LogClass.WriteLog($"{kv.Key} CPT Upload OK.");
                                            }
                                            else
                                            {
                                                //CPT never upload.
                                                LogClass.WriteLog($"{kv.Key} CPT {settle_file} Never Upload,Please Check.");
                                                AlarmTxt($"{kv.Key}  CPT {settle_file} Never Upload,Please Check.");
                                            }

                                            if (conso_amt == total_amt)
                                            {
                                                //CPT same amt
                                                LogClass.WriteLog($"{kv.Key} CPT Settle Same Amount {conso_amt} As Consolidated.");
                                            }
                                            else
                                            {
                                                //CPT diff amt.
                                                LogClass.WriteLog($"{kv.Key} CPT Has Different Amount with Consolidated.conso_amt={conso_amt},settle={total_amt}");
                                                //AlarmTxt($"{kv.Key} CPT Has Different Amount with Consolidated.conso_amt={conso_amt},settle={total_amt}");
                                            }

                                        }
                                    }






                                    if (!flag_cpt)
                                    {
                                        //No CPT file generated.
                                        LogClass.WriteLog($"{kv.Key} Have No CPT File Generated,Please Check.");
                                        AlarmTxt($"{kv.Key} Have No CPT File Amount={conso_amt.ToString()} Generated,Please Check.");
                                    }
                                }

                            }
                            else if (trans_type.Equals("2") && (conso_amt > 0))   //2 CHU Consolidated.
                            {
                                //Update CHU amt;
                                string cmd_update_cpt = @"IF EXISTS (SELECT * FROM [dbo].[DailyRevenueSummary] where Trans_date=@Trans_date and carparkID=@carparkID) 
                                                            BEGIN
                                                               --update
                                                                 update DailyRevenueSummary set CHU_conso=@CHU_conso,CHU_settle=@CHU_settle,update_date=getdate(),ipgroup=@ipgroup where Trans_date=@Trans_date and carparkID=@carparkID
                                                            END
                                                            ELSE
                                                            BEGIN
                                                               -- insert
                                                               Insert INTO DailyRevenueSummary(carparkID,batch,CHU_conso,CHU_settle,Trans_date,update_date,ipgroup)VALUES(@carparkID,@batch,@CHU_conso,@CHU_settle,@Trans_date,getdate(),@ipgroup)
                                                            END";

                                SqlParameter[] para_cpt = new SqlParameter[]
                                {
                                        new SqlParameter("@carparkID",kv.Key),
                                        new SqlParameter("@batch",batchlist[kv.Key]),
                                        new SqlParameter("@CHU_conso",conso_amt),
                                        new SqlParameter("@CHU_settle",settle_amt),
                                        new SqlParameter("@ipgroup",ipgroup),
                                        new SqlParameter("@Trans_date",trans_date)
                                };

                                try
                                {
                                    SqlHelper.ExecuteNonQuery(constr_server, CommandType.Text, cmd_update_cpt, para_cpt);
                                    LogClass.WriteLog("Insert Data to Server Ok.");
                                }
                                catch (SqlException sqle)
                                {
                                    LogClass.WriteLog($"Found Error When Insert To Server. {sqle.ToString()}");
                                    //AlarmTxt($"{kv.Key} Have No CPT File Amount={conso_amt.ToString()} Generated,Please Check.");
                                }




                                if (ds.Tables[0].Rows.Count > 0)
                                {
                                    foreach (DataRow dr_2 in ds.Tables[0].Rows)
                                    {
                                        string settle_sys = dr_2["settle_sys"].ToString();
                                        string settle_file = dr_2["settle_file"].ToString();
                                        string send_flag = dr_2["send_flag"].ToString();
                                        double total_amt = Convert.ToDouble(dr_2["total_amt"].ToString());
                                        if (settle_sys.Equals("1"))   //1 CHU Settled.
                                        {
                                            flag_chu = true;
                                            if (send_flag.Equals("1"))
                                            {
                                                //CHU upload ok.
                                                LogClass.WriteLog($"{kv.Key} CHU Upload OK.");
                                            }
                                            else
                                            {
                                                //CHU never upload.
                                                LogClass.WriteLog($"{kv.Key} CHU {settle_file} Never Upload,Please Check.");
                                                AlarmTxt($"{kv.Key} CHU {settle_file} Never Upload,Please Check.");
                                            }

                                            if (conso_amt == total_amt)
                                            {
                                                //CHU same amt
                                                LogClass.WriteLog($"{kv.Key} CHU Settle Same Amount {conso_amt} As Consolidated.");
                                            }
                                            else
                                            {
                                                //CHU diff amt.
                                                LogClass.WriteLog($"{kv.Key} CHU Has Different Amount with Consolidated.conso_amt={conso_amt},settle={total_amt}");
                                                //AlarmTxt($"{kv.Key} CHU Has Different Amount with Consolidated.conso_amt={conso_amt},settle={total_amt}");
                                            }
                                        }
                                    }
                                }

                                //Update CHU amt;

                                if (!flag_chu)
                                {
                                    if (isCHUIPSame)
                                    {
                                        //No CHU file generated.
                                        LogClass.WriteLog($"{kv.Key} Have No CHU File Generated,Please Check.");
                                        AlarmTxt($"{kv.Key} Have No CHU File Amount={conso_amt.ToString()} Generated,Please Check.");

                                    }
                                    else
                                    {
                                        //CHUIP and PMS not same IP.
                                        LogClass.WriteLog($"{kv.Key} CHU File Generated From Other PMS {value}.");
                                    }

                                }

                            }

                        }
                    }
                    else
                    {
                        LogClass.WriteLog($"No Nets data can collect.");
                    }
                }
                catch (Exception e)
                {

                }
            }

        }
        private void CheckSettlementFilesAtServer()
        {
            foreach (KeyValuePair<string, string> fb in SettleFileBatch)  // key is filename,value is batchName.
            {
                string batchName = fb.Value;
                string fileName = fb.Key;
                string ServerIP = BatchNetsServer[batchName].ToString();
                string Filelocation = SettleFileName[fb.Key].ToString();
                try
                {
                    if (CheckFileUpload(fileName, ServerIP))
                    {
                        //File upload ok at Nets Server
                        LogClass.WriteLog($"{fileName} {Filelocation} Upload OK At Server {ServerIP}");
                    }
                    else
                    {
                        //File never upload at Nets Server, please check PMS/Server.
                        LogClass.WriteLog($"{fileName} {Filelocation} Never Upload At Server {ServerIP}");
                        AlarmTxt($"{fileName} {Filelocation} Never Upload At Server {ServerIP}");
                    }
                }
                catch (Exception e)
                {
                    LogClass.WriteLog($"Found Error When Check On Server {ServerIP}, {e.ToString()}");
                    AlarmTxt($"Found Error When Check On Server {ServerIP}");

                }
            }
        }
        private bool CheckFileUpload(string fileName, string serverIP)
        {
            bool status = false;
            bool result = false;
            string checkFolderName = @"\\" + serverIP;
            //Connecting share folder.
            try
            {
                status = ShareFolderConnect.connectState(checkFolderName, "sunpark", "Tdxh638*");
            }
            catch (Exception e)
            {
                LogClass.WriteLog($"Can Not Connect To Server {serverIP}" + e.ToString());
                AlarmTxt($"Can Not Connect To Server {serverIP}");
                return result;
            }
            LogClass.WriteLog(checkFolderName + "  connected Ok");
            string FileName = checkFolderName + @"\log\M" + DateTime.Now.ToString("yyyyMMdd") + ".log";
            LogClass.WriteLog(FileName);
            if (status)
            {
                try
                {
                    bool IsUpload = false;
                    // 读取文件的源路径及其读取流
                    StreamReader srReadFile = new StreamReader(FileName);
                    // 读取流直至文件末尾结束
                    while (!srReadFile.EndOfStream)
                    {
                        string strReadLine = srReadFile.ReadLine(); //读取每行数据

                        if (strReadLine.Contains(fileName))
                        {
                            if (strReadLine.Contains("successfully"))
                            {
                                IsUpload = true;
                                break;
                            }
                        }
                    }
                    if (IsUpload)
                    {
                        result = true;
                    }
                    // 关闭读取流文件
                    srReadFile.Close();
                    return result;
                }
                catch (Exception e)
                {
                    LogClass.WriteLog("Fail To Read FileName " + e.ToString());
                    return result;
                }
            }

            try
            {
                throw new NotImplementedException();
            }
            catch (NotImplementedException exece)
            {
                LogClass.WriteLog("NotImplementedException: " + exece.ToString());
                return result;
            }
        }


    }
}
