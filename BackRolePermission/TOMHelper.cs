
/************************************************************************************
 *Copyright (c) 2019 All Rights Reserved.
 *CLR版本： 4.7.2
 *公司名称：菲斯科（上海）软件有限公司
 *机器名称：DEV
 *命名空间：Fisk.DataWithReportUtilities.DBUtility
 *文件名：  TMOHelper
 *版本号：  V1.0.0.0
 *唯一标识：550a192b-b069-4137-bd8f-22d1b483f2df
 *当前的用户域：DENNY
 *创建人：  yhxu
 *创建时间：2019年8月20日16:26:14
 *描述：操作表格模型分析服务方法
/************************************************************************************/
//using Fisk.DataWithReportViewModel;
//using Fisk.Eisai.ViewModel.TabularPermissions;
//using Fisk.Eisai.ViewModel.UserInfo;
using Microsoft.AnalysisServices.AdomdClient;
using Microsoft.AnalysisServices.Tabular;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace BackRolePermission
{
    public class TOMHelper
    {
        //private static string ConnectionString = "DataSource=localhost\\tabular";
        /// <summary>
        /// 连接字符串
        /// </summary>
        private string ConnectionString;
        /// <summary>
        /// 数据库
        /// </summary>
        private string DataBases;
        /// <summary>
        /// 表
        /// </summary>
        private string Table;
        /// <summary>
        /// 行字段
        /// </summary>
        private string Filde;

        public TOMHelper()
        {
        }

        public TOMHelper(string ConnectionString)
        {
            this.ConnectionString = "DataSource=" + ConnectionString;
        }

        public TOMHelper(string ConnectionString, string DataBases)
        {
            this.ConnectionString = "DataSource=" + ConnectionString;
            this.DataBases = DataBases;
        }

        public TOMHelper(string ConnectionString, string DataBases, string Table)
        {
            this.ConnectionString = "DataSource=" + ConnectionString;
            this.DataBases = DataBases;
            this.Table = Table;
        }

        public TOMHelper(string ConnectionString, string DataBases, string Table, string Filde)
        {
            this.ConnectionString = "DataSource=" + ConnectionString;
            this.DataBases = DataBases;
            this.Table = Table;
            this.Filde = Filde;
        }

        /// <summary>
        /// 获取数据库表  2019年8月20日15:46:38  Dennyhui
        /// </summary>
        public DataTable GetTOM_DatabaseList()
        {


            using (Server server = new Server())
            {

                server.Connect(ConnectionString);
                DataTable dtResult = new DataTable();
                dtResult.Columns.Add("name");
                for (int a = 0; a < server.Databases.Count; a++)
                {
                    DataRow dr = dtResult.NewRow();
                    dtResult.Rows.Add(dr);
                    dtResult.Rows[a]["name"] = server.Databases[a].Name;
                }
                DataView dv = new DataView(dtResult);
                DataTable distdt = dv.ToTable(true, "name");
                return distdt;
            }
        }

        /// <summary>
        /// 获取表或列  2019年8月20日15:46:54  Dennyhui
        /// </summary>
        public DataTable getTableDB()
        {
            using (Server server = new Server())
            {

                server.Connect(ConnectionString);
                var db = server.Databases.GetByName(DataBases);
                var tables = db.Model.Tables;
                DataTable dtResult = new DataTable();
                dtResult.Columns.Add("name");
                for (int a = 0; a < tables.Count; a++)
                {
                    DataRow dr = dtResult.NewRow();
                    dtResult.Rows.Add(dr);
                    dtResult.Rows[a]["name"] = tables[a].Name;
                }
                DataView dv = new DataView(dtResult);
                DataTable distdt = dv.ToTable(true, "name");
                return distdt;
            }
        }

        /// <summary>
        /// 获取表下面的列 2019年8月20日18:14:52 王金星
        /// </summary>
        public DataTable getTableColumnFromDB()
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                var db = server.Databases.GetByName(DataBases);
                var Columns = db.Model.Tables[Table].Columns;
                DataTable dtResult = new DataTable();
                dtResult.Columns.Add("name");
                for (int a = 0; a < Columns.Count; a++)
                {
                    DataRow dr = dtResult.NewRow();
                    dtResult.Rows.Add(dr);
                    dtResult.Rows[a]["name"] = Columns[a].Name;
                }
                DataView dv = new DataView(dtResult);
                DataTable distdt = dv.ToTable(true, "name");
                return distdt;
            }
        }

        /// <summary>
        /// 得到笛卡尔积数据 根据MDX 或 DAX
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="mdx"></param>
        /// <returns></returns>
        public dynamic GetCellSet(AdomdConnection conn, string mdx)
        {
            dynamic result = null;
            if (conn != null && conn.State == ConnectionState.Open)
            {
                AdomdCommand command;
                command = new AdomdCommand(mdx, conn);
                result = command.Execute();
                //   command.ExecuteXmlReader();
            }
            return result;
        }

        public AdomdConnection GetConn(string connstr)
        {
            AdomdConnection conn = new AdomdConnection();
            conn.ConnectionString = connstr;
            return conn;
        }

        /// <summary>
        /// 分析服务是否连接成功  2019年8月20日17:27:25  Dennyhui
        /// </summary>
        /// <param name="connStr"></param>
        /// <returns></returns>
        public bool IsSecessConnect(string connStr)
        {
            bool result = false;
            try
            {
                AdomdConnection conn = new AdomdConnection(connStr);
                conn.Open();

                conn.Close();
                result = true;
            }
            catch
            {
                result = false;
            }
            return result;
        }

        ///// <summary>
        ///// 查询维度数据  2019年8月20日15:50:58  Dennyhui
        ///// </summary>
        //public List<TabularData> getTableData()
        //{
        //    var con = GetConn(ConnectionString + ";Catalog=" + DataBases + ";");
        //    con.Open();
        //    var result = GetCellSet(con, "EVALUATE ALL(" + Table + "[" + Filde + "])"); //DAX语句

        //    //DataTable dtResult = new DataTable();
        //    //dtResult.Columns.Add("name");
        //    //foreach (var item in result)
        //    //{
        //    //    DataRow dr = dtResult.NewRow();
        //    //    dr["name"] = item[0];
        //    //    dtResult.Rows.Add(dr);
        //    //}
        //    List<TabularData> listReulst = new List<TabularData>();
        //    foreach (var item in result)
        //    {
        //        TabularData td = new TabularData();
        //        td.name = item[0] + "";
        //        listReulst.Add(td);
        //    }
        //    con.Close();//关闭连接
        //    con.Dispose();//释放资源
        //    Console.ResetColor();
        //    return listReulst;
        //}

        /// <summary>
        /// 表格模型设置权限(角色不存在)  2019年8月26日13:51:23  Dennyhui
        /// </summary>
        public void TabularSetRolePermissionRoleDoesNotExist(List<string> Data, string RoleName, List<string> Userlist, string DomainAccount, string Field)
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                Database asDatabase;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    asDatabase = server.Databases.FindByName(DataBases);
                    //var db = server.Databases.GetByName(DataBases);
                    var Roles = asDatabase.Model.Roles;
                    //添加数据权限dddd

                    TablePermission tpm = new TablePermission();
                    tpm.Table = asDatabase.Model.Tables.Find(Table);
                    //tpm.MetadataPermission = MetadataPermission.None; //表权限


                    //ColumnPermission cpm = new ColumnPermission();
                    //cpm.Column = tpm.Table.Columns.Find("ID_Date");
                    //cpm.MetadataPermission = MetadataPermission.None;
                    //tpm.ColumnPermissions.Add(cpm);

                    StringBuilder filterStr = new StringBuilder();
                    for (int i = 0, ListCount = Data.Count; i < ListCount; i++)
                    {
                        string str = IsNumeric(Data[i]);
                        string stastr = str == "" ? "VALUE(" : "";
                        string Endstr = str == "" ? ")" : "";
                        if (i == ListCount - 1)
                        {
                            filterStr.Append("" + stastr + " " + Table + "[" + Field + "] " + Endstr + "= " + str + "" + Data[i] + "" + str + "  ");
                        }
                        else
                        {
                            filterStr.Append("" + stastr + " " + Table + "[" + Field + "] " + Endstr + " = " + str + "" + Data[i] + "" + str + " || ");
                        }
                    }

                    tpm.FilterExpression = filterStr.ToString();
                    ModelRole mr = new ModelRole();
                    mr.Name = RoleName;
                    mr.ModelPermission = ModelPermission.Read;
                    mr.TablePermissions.Add(tpm);
                    //添加用户
                    if (Userlist.Count > 0)
                    {
                        foreach (string user in Userlist)
                        {
                            ModelRoleMember emrm = new WindowsModelRoleMember();
                            emrm.MemberName = "" + DomainAccount + "\\" + user + "";
                            mr.Members.Add(emrm);
                        }
                    }

                    asDatabase.Model.Roles.Add(mr);
                    asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                }
            }
        }


        ///// <summary>
        ///// 表格模型设置权限(角色存在)  2019年8月26日13:51:23  Dennyhui
        ///// </summary>
        //public void TabularSetRolePermissionRoleExist(List<string> Data, string RoleName, List<Tabular_Role_mapping> TSAlist)
        //{
        //    using (Server server = new Server())
        //    {
        //        server.Connect(ConnectionString);
        //        Database asDatabase;
        //        if (server.Databases.FindByName(DataBases) != null)
        //        {
        //            asDatabase = server.Databases.FindByName(DataBases);
        //            //var db = server.Databases.GetByName(DataBases);
        //            var Roles = asDatabase.Model.Roles;
        //            var thisRole = Roles.Find(RoleName);

        //            StringBuilder filterStr = new StringBuilder();
        //            if (TSAlist == null)
        //            {
        //                if (Data.Count() > 0)
        //                {
        //                    for (int i = 0, ListCount = Data.Count; i < ListCount; i++)
        //                    {
        //                        string str = IsNumeric(Data[i]);
        //                        string stastr = str == "" ? "VALUE(" : "";
        //                        string Endstr = str == "" ? ")" : "";
        //                        if (i == ListCount - 1)
        //                        {

        //                            filterStr.Append("" + stastr + " " + Table + "[" + Filde + "] " + Endstr + " = " + str + "" + Data[i] + "" + str + " ");

        //                        }
        //                        else
        //                        {
        //                            filterStr.Append("" + stastr + " " + Table + "[" + Filde + "] " + Endstr + " = " + str + "" + Data[i] + "" + str + " || ");
        //                        }
        //                    }
        //                }

        //            }
        //            else
        //            {
        //                var OldListcount = TSAlist.GroupBy(e => e.Filed).Select(g => new Filder_DataClass
        //                {
        //                    Filder = g.FirstOrDefault().Filed,
        //                    Data = g.Select(e => e.Data).ToList()
        //                }).ToList();
        //                var FieleCoun = OldListcount.Count();
        //                if (OldListcount.Count() > 0 && Data.Count() > 0)
        //                {
        //                    filterStr.Append("AND(");
        //                    for (int i = 0, ListCount = Data.Count; i < ListCount; i++)
        //                    {
        //                        string str = IsNumeric(Data[i]);
        //                        string stastr = str == "" ? "VALUE(" : "";
        //                        string Endstr = str == "" ? ")" : "";
        //                        if (i == ListCount - 1)
        //                        {
        //                            filterStr.Append("" + stastr + " [" + Filde + "] " + Endstr + " = " + str + "" + Data[i] + "" + str + " ");
        //                        }
        //                        else
        //                        {
        //                            filterStr.Append("" + stastr + " [" + Filde + "] " + Endstr + " = " + str + "" + Data[i] + "" + str + " || ");
        //                        }
        //                    }
        //                    filterStr.Append(TraversalDax(OldListcount));
        //                    var FilderCount = FieleCoun + 1;
        //                    int SupportCount = 0;
        //                    SupportCount = FilderCount - 1;
        //                    for (int i = 0; i < SupportCount; i++)
        //                    {
        //                        filterStr.Append(" )");
        //                    }
        //                }
        //                else if (OldListcount.Count() > 1 && Data.Count() == 0)
        //                {
        //                    filterStr.Append("AND(");
        //                    for (int i = 0, ListCount = OldListcount[0].Data.Count; i < ListCount; i++)
        //                    {
        //                        string str = IsNumeric(OldListcount[0].Data[i]);
        //                        string stastr = str == "" ? "VALUE(" : "";
        //                        string Endstr = str == "" ? ")" : "";
        //                        if (i == ListCount - 1)
        //                        {

        //                            filterStr.Append("" + stastr + " [" + OldListcount[0].Filder + "] " + Endstr + " = " + str + "" + OldListcount[0].Data[i] + "" + str + " ");

        //                        }
        //                        else
        //                        {
        //                            filterStr.Append("" + stastr + " [" + OldListcount[0].Filder + "] " + Endstr + " = " + str + "" + OldListcount[0].Data[i] + "" + str + " || ");
        //                        }
        //                    }
        //                    OldListcount.RemoveAt(0);
        //                    filterStr.Append(TraversalDax(OldListcount));
        //                    var FilderCount = FieleCoun;
        //                    int SupportCount = 0;
        //                    SupportCount = FilderCount - 1;
        //                    for (int i = 0; i < SupportCount; i++)
        //                    {
        //                        filterStr.Append(" )");
        //                    }

        //                }
        //                else if (OldListcount.Count() == 1)
        //                {
        //                    for (int i = 0, ListCount = OldListcount[0].Data.Count; i < ListCount; i++)
        //                    {
        //                        string str = IsNumeric(OldListcount[0].Data[i]);
        //                        string stastr = str == "" ? "VALUE(" : "";
        //                        string Endstr = str == "" ? ")" : "";
        //                        if (i == ListCount - 1)
        //                        {
        //                            filterStr.Append("" + stastr + " " + Table + "[" + OldListcount[0].Filder + "] " + Endstr + " = " + str + "" + OldListcount[0].Data[i] + "" + str + " ");
        //                        }
        //                        else
        //                        {
        //                            filterStr.Append("" + stastr + " " + Table + "[" + OldListcount[0].Filder + "] " + Endstr + " = " + str + "" + OldListcount[0].Data[i] + "" + str + " || ");
        //                        }
        //                    }
        //                }
        //            }


        //            if (thisRole.TablePermissions.Find(Table) == null)
        //            {
        //                TablePermission tpm = new TablePermission();
        //                tpm.Table = asDatabase.Model.Tables.Find(Table);

        //                tpm.FilterExpression = filterStr.ToString();
        //                thisRole.TablePermissions.Add(tpm);
        //            }
        //            else
        //            {
        //                thisRole.TablePermissions.Find(Table).FilterExpression = filterStr.ToString();
        //            }
        //            asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        //        }
        //    }
        //}


        /// <summary>
        /// 表格模型设置权限(角色存在)  2019年8月26日13:51:23  Dennyhui
        /// </summary>
        //public void TabularSetRolePermissionRoleExist(List<string> Data, string RoleName, List<string> Userlist)
        //{
        //    using (Server server = new Server())
        //    {
        //        server.Connect(ConnectionString);
        //        Database asDatabase;
        //        if (server.Databases.FindByName(DataBases) != null)
        //        {
        //            asDatabase = server.Databases.FindByName(DataBases);
        //            //var db = server.Databases.GetByName(DataBases);
        //            var Roles = asDatabase.Model.Roles;
        //            var thisRole = Roles.Find(RoleName);

        //            StringBuilder filterStr = new StringBuilder();
        //            var OldListcount = TSAlist.GroupBy(e => e.Field).Select(g => new Filder_DataClass
        //            {
        //                Filder = g.FirstOrDefault().Field,
        //                Data = g.Select(e => e.Data).ToList()
        //            }).ToList();
        //            var FieleCoun = OldListcount.Count();
        //            if (OldListcount.Count() > 0 && Data.Count() > 0)
        //            {
        //                filterStr.Append("AND(");
        //                for (int i = 0, ListCount = Data.Count; i < ListCount; i++)
        //                {
        //                    string str = IsNumeric(Data[i]);
        //                    string stastr = str == "" ? "VALUE(" : "";
        //                    string Endstr = str == "" ? ")" : "";
        //                    if (i == ListCount - 1)
        //                    {
        //                        filterStr.Append("" + stastr + " [" + Filde + "] " + Endstr + " = " + str + "" + Data[i] + "" + str + " ");
        //                    }
        //                    else
        //                    {
        //                        filterStr.Append("" + stastr + " [" + Filde + "] " + Endstr + " = " + str + "" + Data[i] + "" + str + " || ");
        //                    }
        //                }
        //                filterStr.Append(TraversalDax(OldListcount));
        //                var FilderCount = FieleCoun + 1;
        //                int SupportCount = 0;
        //                SupportCount = FilderCount - 1;
        //                for (int i = 0; i < SupportCount; i++)
        //                {
        //                    filterStr.Append(" )");
        //                }
        //            }
        //            else if (OldListcount.Count() > 1 && Data.Count() == 0)
        //            {
        //                filterStr.Append("AND(");
        //                for (int i = 0, ListCount = OldListcount[0].Data.Count; i < ListCount; i++)
        //                {
        //                    string str = IsNumeric(OldListcount[0].Data[i]);
        //                    string stastr = str == "" ? "VALUE(" : "";
        //                    string Endstr = str == "" ? ")" : "";
        //                    if (i == ListCount - 1)
        //                    {

        //                        filterStr.Append("" + stastr + " [" + OldListcount[0].Filder + "] " + Endstr + " = " + str + "" + OldListcount[0].Data[i] + "" + str + " ");

        //                    }
        //                    else
        //                    {
        //                        filterStr.Append("" + stastr + " [" + OldListcount[0].Filder + "] " + Endstr + " = " + str + "" + OldListcount[0].Data[i] + "" + str + " || ");
        //                    }
        //                }
        //                OldListcount.RemoveAt(0);
        //                filterStr.Append(TraversalDax(OldListcount));
        //                var FilderCount = FieleCoun;
        //                int SupportCount = 0;
        //                SupportCount = FilderCount - 1;
        //                for (int i = 0; i < SupportCount; i++)
        //                {
        //                    filterStr.Append(" )");
        //                }

        //            }
        //            else if (OldListcount.Count() == 1)
        //            {
        //                for (int i = 0, ListCount = OldListcount[0].Data.Count; i < ListCount; i++)
        //                {
        //                    string str = IsNumeric(OldListcount[0].Data[i]);
        //                    string stastr = str == "" ? "VALUE(" : "";
        //                    string Endstr = str == "" ? ")" : "";
        //                    if (i == ListCount - 1)
        //                    {
        //                        filterStr.Append("" + stastr + " " + Table + "[" + OldListcount[0].Filder + "] " + Endstr + " = " + str + "" + OldListcount[0].Data[i] + "" + str + " ");
        //                    }
        //                    else
        //                    {
        //                        filterStr.Append("" + stastr + " " + Table + "[" + OldListcount[0].Filder + "] " + Endstr + " = " + str + "" + OldListcount[0].Data[i] + "" + str + " || ");
        //                    }
        //                }
        //            }
        //            else if (Data.Count() > 0)
        //            {
        //                for (int i = 0, ListCount = Data.Count; i < ListCount; i++)
        //                {
        //                    string str = IsNumeric(Data[i]);
        //                    string stastr = str == "" ? "VALUE(" : "";
        //                    string Endstr = str == "" ? ")" : "";
        //                    if (i == ListCount - 1)
        //                    {

        //                        filterStr.Append("" + stastr + " " + Table + "[" + Filde + "] " + Endstr + " = " + str + "" + Data[i] + "" + str + " ");

        //                    }
        //                    else
        //                    {
        //                        filterStr.Append("" + stastr + " " + Table + "[" + Filde + "] " + Endstr + " = " + str + "" + Data[i] + "" + str + " || ");
        //                    }
        //                }
        //            }

        //            if (thisRole.TablePermissions.Find(Table) == null)
        //            {
        //                TablePermission tpm = new TablePermission();
        //                tpm.Table = asDatabase.Model.Tables.Find(Table);
        //                tpm.FilterExpression = filterStr.ToString();
        //                thisRole.TablePermissions.Add(tpm);
        //            }
        //            else
        //            {
        //                thisRole.TablePermissions.Find(Table).FilterExpression = filterStr.ToString();
        //            }
        //            asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        //        }
        //    }
        //}

        /// <summary>
        /// 判断是否为数字
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public string IsNumeric(string value)
        {
            string IsNumber = string.Empty;
            if (Regex.IsMatch(value, @"^[+-]?\d*[.]?\d*$"))
            {
                IsNumber = "";
            }
            else
            {
                IsNumber = "\"";
            }
            return IsNumber;
        }

        /// <summary>
        /// 判断角色是否已被写入 2019年8月26日17:24:50 王金星
        /// </summary>
        /// <returns></returns>
        public bool RoleIsHave(string RoleName)
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                Database asDatabase;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    asDatabase = server.Databases.FindByName(DataBases);
                    //var db = server.Databases.GetByName(DataBases);
                    var Roles = asDatabase.Model.Roles;
                    var RoleIsHave = false;
                    //foreach (var Roleitem in Roles)
                    //{
                    //    if (Roleitem.Name.Equals(RoleName))
                    //    {
                    //        RoleIsHave = true;
                    //    }
                    //}
                    RoleIsHave = Roles.Where(e => e.Name == RoleName).Any();
                    return RoleIsHave;
                }
                else
                {
                    return false;
                }
            }
        }

        ///// <summary>
        ///// 删除数据库下的角色 2019年8月28日17:20:07 王金星
        ///// </summary>
        //public void DelTabularDatabaseRole(List<Tabular_Role_List> tsaList, string RoleName)
        //{

        //    foreach (var item in tsaList)
        //    {
        //        using (Server server = new Server())
        //        {
        //            server.Connect("DataSource=" + item.Instance);
        //            Database database;
        //            if (server.Databases.FindByName(item.DataBases) != null)
        //            {
        //                database = server.Databases.FindByName(item.DataBases);
        //                var roles = database.Model.Roles;
        //                if (roles.Find(RoleName) != null)
        //                {
        //                    roles.Remove(RoleName);

        //                }
        //                database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        //            }
        //        }
        //    }
        //}

        ///// <summary>
        ///// 修改数据库下面的角色名称 2019年8月28日17:20:15 王金星
        ///// </summary>
        //public void UpdateTbularDataBaseRole(List<Tabular_Role_List> tsaList, string RoleName, string NewRoleName)
        //{

        //    foreach (var item in tsaList)
        //    {
        //        using (Server server = new Server())
        //        {
        //            server.Connect("DataSource=" + item.Instance);
        //            Database database;
        //            if (server.Databases.FindByName(item.DataBases) != null)
        //            {
        //                database = server.Databases.FindByName(item.DataBases);
        //                var roles = database.Model.Roles;
        //                if (roles.Find(RoleName) != null)
        //                {
        //                    roles.Find(RoleName).RequestRename(NewRoleName);
        //                    database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        //                }
        //            }
        //        }
        //    }
        //}

        ///// <summary>
        ///// 向角色下面添加成员 2019年8月28日17:20:22 王金星
        ///// </summary>
        //public void AddMembers(List<Tabular_Role_List> tsa, List<Role_User> ru_list, string Account)
        //{

        //    foreach (var item in tsa)
        //    {
        //        using (Server server = new Server())
        //        {
        //            server.Connect("DataSource=" + item.Instance);
        //            Database database;
        //            if (server.Databases.FindByName(item.DataBases) != null)
        //            {
        //                database = server.Databases.FindByName(item.DataBases);
        //                var roles = database.Model.Roles;
        //                var thisrole = roles.Find(item.RoleName);
        //                if (thisrole != null)
        //                {
        //                    ModelRoleMember mb = new WindowsModelRoleMember();
        //                    mb.MemberName = "" + Account + "";
        //                    if (!thisrole.Members.ContainsName(mb.MemberName))
        //                    {
        //                        thisrole.Members.Add(mb);
        //                    }
        //                    database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        //                    server.Disconnect();
        //                }
        //                else
        //                {
        //                    ModelRole mr = new ModelRole();
        //                    mr.Name = item.RoleName;
        //                    mr.ModelPermission = ModelPermission.Read;
        //                    var Role_User_list = ru_list.Where(e => e.RoleName == item.RoleName).ToList();
        //                    //添加用户
        //                    foreach (var user in Role_User_list)
        //                    {
        //                        ModelRoleMember emrm = new WindowsModelRoleMember();
        //                        emrm.MemberName = "" + user.UserName + "";
        //                        mr.Members.Add(emrm);
        //                    }
        //                    ModelRoleMember emrm11 = new WindowsModelRoleMember();
        //                    emrm11.MemberName = "" + Account + "";
        //                    mr.Members.Add(emrm11);
        //                    database.Model.Roles.Add(mr);
        //                    database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        //                }
        //            }
        //        }
        //    }
        //}

        ///// <summary>
        ///// 删除角色下的成员 2019年8月28日17:20:35 王金星
        ///// </summary>
        //public void del_Menber(List<Tabular_Role_List> tsa, string Account)
        //{
        //    foreach (var item in tsa)
        //    {
        //        using (Server server = new Server())
        //        {
        //            server.Connect("DataSource=" + item.Instance);
        //            Database database;
        //            if (server.Databases.FindByName(item.DataBases) != null)
        //            {
        //                database = server.Databases.FindByName(item.DataBases);
        //                var roles = database.Model.Roles;
        //                var thisrole = roles.Find(item.RoleName);
        //                if (thisrole != null)
        //                {
        //                    //var TabularAccount = "" + ConfigurationManager.AppSettings["TabularAreaName"].ToString() + "\\" + Account + "#default";
        //                    var TabularAccount = "" + Account;
        //                    if (thisrole.Members.ContainsName(TabularAccount))
        //                    {
        //                        thisrole.Members.Remove(TabularAccount);
        //                    }
        //                    database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        //                }
        //            }
        //            server.Disconnect();
        //            server.Dispose();
        //        }
        //    }
        //}

        /// <summary>
        /// 删除角色下的成员在AD中 2019年8月28日17:20:35 王金星
        /// </summary>
        //public void del_MenberInAd(List<Tabular_Role_List> tsa, string Account)
        //{
        //    foreach (var item in tsa)
        //    {
        //        using (Server server = new Server())
        //        {
        //            server.Connect("DataSource=" + item.Instance);
        //            Database database;
        //            if (server.Databases.FindByName(item.DataBases) != null)
        //            {
        //                database = server.Databases.FindByName(item.DataBases);
        //                var roles = database.Model.Roles;
        //                var thisrole = roles.Find(item.RoleName);
        //                if (thisrole != null)
        //                {
        //                    //var TabularAccount = "" + ConfigurationManager.AppSettings["TabularAreaName"].ToString() + "\\" + Account + "#default";

        //                    if (thisrole.Members.ContainsName(Account))
        //                    {
        //                        thisrole.Members.Remove(Account);
        //                    }
        //                    database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        //                }
        //            }
        //            server.Disconnect();
        //            server.Dispose();
        //        }
        //    }
        //}

        /// <summary>
        /// 删除角色下的多个成员 2019年8月28日17:20:35 王金星
        /// </summary>
        //public void del_RoleMenbers(List<Tabular_Role_List> tsa, List<string> Account)
        //{
        //    foreach (var item in tsa)
        //    {
        //        using (Server server = new Server())
        //        {
        //            server.Connect("DataSource=" + item.Instance);
        //            Database database;
        //            if (server.Databases.FindByName(item.DataBases) != null)
        //            {
        //                database = server.Databases.FindByName(item.DataBases);
        //                var roles = database.Model.Roles;
        //                var thisrole = roles.Find(item.RoleName);
        //                if (thisrole != null)
        //                {
        //                    foreach (var user in Account)
        //                    {
        //                        //var TabularAccount = "" + ConfigurationManager.AppSettings["TabularAreaName"].ToString() + "\\" + Account + "#default";
        //                        var TabularAccount = "" + ConfigurationManager.AppSettings["TabularAreaName"].ToString() + "\\" + user;
        //                        if (thisrole.Members.ContainsName(TabularAccount))
        //                        {
        //                            thisrole.Members.Remove(TabularAccount);
        //                        }
        //                    }
        //                    database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        //                }
        //            }
        //            server.Disconnect();
        //            server.Dispose();
        //        }
        //    }
        //}

        /// <summary>
        /// 向角色下面添加多个成员 2019年8月28日17:20:22 王金星
        /// </summary>
        //public void AddMultipleUsersUnderTheRole(List<Tabular_Role_List> tsa, List<string> Account)
        //{

        //    foreach (var item in tsa)
        //    {
        //        using (Server server = new Server())
        //        {
        //            server.Connect("DataSource=" + item.Instance);
        //            Database database;
        //            if (server.Databases.FindByName(item.DataBases) != null)
        //            {
        //                database = server.Databases.FindByName(item.DataBases);
        //                var roles = database.Model.Roles;
        //                var thisrole = roles.Find(item.RoleName);
        //                if (thisrole != null)
        //                {
        //                    foreach (var user in Account)
        //                    {
        //                        ModelRoleMember mb = new WindowsModelRoleMember();
        //                        mb.MemberName = "" + ConfigurationManager.AppSettings["TabularAreaName"].ToString() + "\\" + user + "";
        //                        if (!thisrole.Members.ContainsName(mb.MemberName))
        //                        {
        //                            thisrole.Members.Add(mb);
        //                        }
        //                    }
        //                    database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        //                }
        //            }
        //            server.Disconnect();
        //            server.Dispose();
        //        }
        //    }
        //}


        ///// <summary>
        ///// 删除角色下的成员 2019年8月28日17:20:35 王金星
        ///// </summary>
        //public void del_Menber(List<Tabular_Role_List> tsa, string Account)
        //{
        //    using (Server server = new Server())
        //    {
        //        foreach (var item in tsa)
        //        {
        //            server.Connect("DataSource=" + item.Instance);
        //            Database database;
        //            database = server.Databases.FindByName(item.DataBases);
        //            var roles = database.Model.Roles;
        //            var thisrole = roles.Find(item.RoleName);
        //            string ad = ConfigurationManager.AppSettings["TabularAreaName"].ToString();
        //            string newAccount = string.Empty;
        //            if (!Account.Contains(ad))
        //            {
        //                newAccount = "" + ad + "\\" + Account + "";
        //            }
        //            else
        //            {
        //                newAccount = Account;
        //            }
        //            foreach (var item2 in thisrole.Members)
        //            {
        //                if (item2.MemberName == newAccount)
        //                {
        //                    thisrole.Members.Remove(item2.Name);
        //                    database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        //                    database.Dispose();
        //                    server.Disconnect();
        //                    server.Dispose();
        //                }

        //            }

        //        }
        //    }
        //}

        /// <summary>
        /// 删除实例下全部数据库的角色
        /// </summary>
        public void delete_Insert(string Insert)
        {
            using (Server server = new Server())
            {
                server.Connect("DataSource=" + Insert);
                for (int a = 0; a < server.Databases.Count; a++)
                {
                    var dataname = server.Databases[a].Name;
                    if (server.Databases.FindByName(dataname) != null)
                    {
                        var database = server.Databases.FindByName(dataname);
                        var roles = database.Model.Roles;
                        foreach (var item in roles)
                        {
                            roles.Remove(item.Name);

                        }
                        database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                    }
                }
            }
        }

        /// <summary>
        /// 删除数据库下面的角色
        /// </summary>
        /// <param name="table"></param>
        public void delete_DataBase(string Insert, string DataBase, DataTable notDelRole)
        {
            using (Server server = new Server())
            {
                server.Connect("DataSource=" + Insert);
                if (server.Databases.FindByName(DataBase) != null)
                {
                    var database = server.Databases.FindByName(DataBase);
                    var roles = database.Model.Roles;
                    foreach (var item in roles)
                    {
                        var delRole = notDelRole.Select($"RoleName='{item.Name}'").FirstOrDefault();
                        //if (delRole == null)
                        //{
                        roles.Remove(item.Name);
                        //}
                    }
                    database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                }
            }
        }

        /// <summary>
        /// 删除数据库下的表格权限
        /// </summary>
        /// <param name="Inser"></param>
        /// <param name="DataBase"></param>
        /// <param name="Table"></param>
        public void delelte_table(string Insert, string DataBase, string Table)
        {
            using (Server server = new Server())
            {
                server.Connect("DataSource=" + Insert);
                if (server.Databases.FindByName(DataBase) != null)
                {
                    var database = server.Databases.FindByName(DataBase);
                    var roles = database.Model.Roles;
                    foreach (var item in roles)
                    {
                        if (item.TablePermissions.Find(Table) != null)
                        {
                            item.TablePermissions.Find(Table).FilterExpression = "";
                        }

                    }
                    database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                }
            }
        }

        /// <summary>
        /// 删除当前字段的权限
        /// </summary>
        /// <param name="Insert"></param>
        /// <param name="DataBase"></param>
        /// <param name="Table"></param>
        /// <param name="Filer"></param>
        //public void Delete_Filer(string Insert, string DataBase, string Table, string Filer, List<Tabular_Security_Access_RoleNam> TSAlist)
        //{
        //    using (Server server = new Server())
        //    {
        //        server.Connect("DataSource=" + Insert);
        //        if (server.Databases.FindByName(DataBase) != null)
        //        {
        //            var database = server.Databases.FindByName(DataBase);
        //            var roles = database.Model.Roles;
        //            var Add_tabular_Powert_role = TSAlist.GroupBy(e => e.RoleName).Select(e => new Role_Filter
        //            {
        //                Filder = e.Select(c => c.Field).Distinct().ToList(),
        //                RoleName = e.FirstOrDefault().RoleName,
        //            });
        //            foreach (var item in roles)
        //            {
        //                foreach (var item2 in Add_tabular_Powert_role)
        //                {
        //                    if (item.Name == item2.RoleName)
        //                    {

        //                        item2.Filder.Remove(Filer);
        //                        if (item2.Filder.Count > 0)
        //                        {
        //                            StringBuilder filterStr = new StringBuilder();
        //                            var OldListcount = TSAlist.Where(e => e.RoleName == item.Name && item2.Filder.Contains(e.Field)).GroupBy(e => e.Field).Select(g => new Filder_DataClass
        //                            {
        //                                Filder = g.FirstOrDefault().Field,
        //                                Data = g.Select(e => e.Data).ToList()
        //                            }).ToList();
        //                            var FieleCoun = OldListcount.Count();
        //                            if (OldListcount.Count == 1)
        //                            {
        //                                for (int i = 0, ListCount = OldListcount[0].Data.Count; i < ListCount; i++)
        //                                {
        //                                    string str = IsNumeric(OldListcount[0].Data[i]);
        //                                    string stastr = str == "" ? "VALUE(" : "";
        //                                    string Endstr = str == "" ? ")" : "";
        //                                    if (i == ListCount - 1)
        //                                    {
        //                                        filterStr.Append("" + stastr + " " + Table + "[" + OldListcount[0].Filder + "] " + Endstr + " = " + str + "" + OldListcount[0].Data[i] + "" + str + " ");
        //                                    }
        //                                    else
        //                                    {
        //                                        filterStr.Append("" + stastr + " " + Table + "[" + OldListcount[0].Filder + "] " + Endstr + "= " + str + "" + OldListcount[0].Data[i] + "" + str + " || ");
        //                                    }
        //                                }
        //                            }
        //                            else if (OldListcount.Count > 1)
        //                            {
        //                                filterStr.Append("AND(");
        //                                for (int i = 0, ListCount = OldListcount[0].Data.Count; i < ListCount; i++)
        //                                {
        //                                    string str = IsNumeric(OldListcount[0].Data[i]);
        //                                    string stastr = str == "" ? "VALUE(" : "";
        //                                    string Endstr = str == "" ? ")" : "";
        //                                    if (i == ListCount - 1)
        //                                    {

        //                                        filterStr.Append("" + stastr + " [" + OldListcount[0].Filder + "] " + Endstr + " = " + str + "" + OldListcount[0].Data[i] + "" + str + " ");

        //                                    }
        //                                    else
        //                                    {
        //                                        filterStr.Append("" + stastr + " [" + OldListcount[0].Filder + "] " + Endstr + "= " + str + "" + OldListcount[0].Data[i] + "" + str + " || ");
        //                                    }
        //                                }
        //                                OldListcount.RemoveAt(0);
        //                                filterStr.Append(TraversalDax(OldListcount));
        //                                var FilderCount = FieleCoun;
        //                                int SupportCount = 0;
        //                                SupportCount = FilderCount - 1;
        //                                for (int i = 0; i < SupportCount; i++)
        //                                {
        //                                    filterStr.Append(" )");
        //                                }
        //                            }
        //                            item.TablePermissions.Find(Table).FilterExpression = filterStr.ToString();
        //                        }
        //                        else
        //                        {
        //                            item.TablePermissions.Find(Table).FilterExpression = "";
        //                        }

        //                    }
        //                }
        //            }
        //            database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        //        }
        //    }
        //}

        ///// <summary>
        ///// 通过递归拼接DAX语句
        ///// </summary>
        ///// <param name="OldListcount"></param>
        ///// <returns></returns>
        //public string TraversalDax(List<Filder_DataClass> OldListcount)
        //{
        //    StringBuilder sbdax = new StringBuilder();
        //    sbdax.Append(" , ");
        //    if (OldListcount.Count() == 1)
        //    {
        //        var AList = OldListcount[0];
        //        for (int i = 0, ListCount = AList.Data.Count(); i < ListCount; i++)
        //        {
        //            string str = IsNumeric(AList.Data[i]);
        //            string stastr = str == "" ? "VALUE(" : "";
        //            string Endstr = str == "" ? ")" : "";
        //            if (i == ListCount - 1)
        //            {
        //                sbdax.Append("" + stastr + " [" + AList.Filder + "] " + Endstr + "= " + str + "" + AList.Data[i] + "" + str + " ");
        //            }
        //            else
        //            {
        //                sbdax.Append("" + stastr + " [" + AList.Filder + "] " + Endstr + "= " + str + "" + AList.Data[i] + "" + str + " || ");
        //            }
        //        }
        //        OldListcount.RemoveRange(0, 1);
        //    }
        //    else
        //    {
        //        var AList = OldListcount[0];
        //        sbdax.Append(" AND( ");
        //        for (int i = 0, ListCount = AList.Data.Count(); i < ListCount; i++)
        //        {
        //            string str = IsNumeric(AList.Data[i]);
        //            string stastr = str == "" ? "VALUE(" : "";
        //            string Endstr = str == "" ? ")" : "";
        //            if (i == ListCount - 1)
        //            {
        //                sbdax.Append("" + stastr + " [" + AList.Filder + "] " + Endstr + "= " + str + "" + AList.Data[i] + "" + str + " ");
        //            }
        //            else
        //            {
        //                sbdax.Append("" + stastr + " [" + AList.Filder + "] " + Endstr + "= " + str + "" + AList.Data[i] + "" + str + " || ");
        //            }
        //        }

        //        OldListcount.RemoveRange(0, 1);
        //        if (OldListcount.Count() >= 2)
        //        {
        //            sbdax.Append(" , AND( ");
        //        }
        //        else
        //        {
        //            sbdax.Append(" , ");

        //        }
        //        var AList1 = OldListcount[0];
        //        for (int i = 0, ListCount = AList1.Data.Count(); i < ListCount; i++)
        //        {
        //            string str = IsNumeric(AList1.Data[i]);
        //            string stastr = str == "" ? "VALUE(" : "";
        //            string Endstr = str == "" ? ")" : "";
        //            if (i == ListCount - 1)
        //            {
        //                sbdax.Append("" + stastr + " [" + AList1.Filder + "] " + Endstr + "= " + str + "" + AList1.Data[i] + "" + str + " ");
        //            }
        //            else
        //            {
        //                sbdax.Append("" + stastr + " [" + AList1.Filder + "] " + Endstr + "= " + str + "" + AList1.Data[i] + "" + str + " || ");
        //            }
        //        }
        //        OldListcount.RemoveRange(0, 1);
        //    }
        //    if (OldListcount.Count > 0)
        //    {
        //        sbdax.Append(TraversalDax(OldListcount));
        //    }
        //    return sbdax.ToString();
        //}

        //public string GetDataBaseRole()
        //{
        //    using (Server server = new Server())
        //    {
        //        server.Connect(ConnectionString);
        //        Database asDatabase;
        //        if (server.Databases.FindByName(DataBases) != null)
        //        {
        //            asDatabase = server.Databases.FindByName(DataBases);
        //            var Roles = asDatabase.Model.Roles;
        //            foreach (var item in Roles)
        //            {
        //                Roles["name"]
        //            }
        //        }
        //    }
        //}

        /// <summary>
        /// 控制列权限
        /// </summary>
        /// <returns></returns>
        public dynamic DaxFieldToRole()
        {
            var obj = new object();

            string dax = "{\"createOrReplace\": { \"object\": {\"database\": \"AdventureWorksDW2016_Tabular\",\"role\": \"Role 2\"},\"role\": {\"name\": \"Role 2\",\"modelPermission\": \"read\",\"tablePermissions\": [{\"name\": \"DimCustomer\",}]}}}";
            var con = GetConn(ConnectionString + ";Catalog=" + DataBases + ";");
            con.Open();
            if (con != null && con.State == ConnectionState.Open)
            {
                AdomdCommand command;
                command = new AdomdCommand(dax, con);
                var testt = command.ExecuteXmlReader();
                //   command.ExecuteXmlReader();
            }
            con.Close();//关闭连接
            con.Dispose();//释放资源
            return obj;

        }


        /// <summary>
        /// 设置表权限 （角色存在）
        /// </summary>
        /// <param name="RoleName"></param>
        /// <param name="TableList"></param>
        public void SetTablePermissionExist(string RoleName, List<string> TableList)
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                Database asDatabase;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    asDatabase = server.Databases.FindByName(DataBases);
                    foreach (var AddTable in TableList)
                    {
                        //var db = server.Databases.GetByName(DataBases);
                        var Roles = asDatabase.Model.Roles;
                        var thisRole = Roles.Find(RoleName);

                        if (thisRole.TablePermissions.Find(AddTable) == null)
                        {
                            TablePermission tpm = new TablePermission();
                            tpm.Table = asDatabase.Model.Tables.Find(AddTable);
                            tpm.MetadataPermission = MetadataPermission.None;
                            thisRole.TablePermissions.Add(tpm);
                        }
                        else
                        {
                            thisRole.TablePermissions.Find(AddTable).MetadataPermission = MetadataPermission.None;
                        }
                        asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                    }

                }
            }
        }

        ///// <summary>
        ///// 设置表权限 （角色不存在）
        ///// </summary>
        ///// <param name="RoleName"></param>
        ///// <param name="TableList"></param>
        //public void SetTablePermissionNotExist(string RoleName, List<string> TableList, List<string> Userlist)
        //{
        //    using (Server server = new Server())
        //    {
        //        server.Connect(ConnectionString);
        //        Database asDatabase;
        //        if (server.Databases.FindByName(DataBases) != null)
        //        {
        //            asDatabase = server.Databases.FindByName(DataBases);

        //            foreach (var AddTable in TableList)
        //            {

        //                //var db = server.Databases.GetByName(DataBases);
        //                var Roles = asDatabase.Model.Roles;
        //                //添加数据权限

        //                TablePermission tpm = new TablePermission();
        //                tpm.Table = asDatabase.Model.Tables.Find(AddTable);
        //                tpm.MetadataPermission = MetadataPermission.None; //表权限


        //                //ColumnPermission cpm = new ColumnPermission();
        //                //cpm.Column = tpm.Table.Columns.Find("ID_Date");
        //                //cpm.MetadataPermission = MetadataPermission.None;
        //                //tpm.ColumnPermissions.Add(cpm);


        //                ModelRole mr = new ModelRole();
        //                mr.Name = RoleName;
        //                mr.ModelPermission = ModelPermission.Read;
        //                mr.TablePermissions.Add(tpm);
        //                //添加用户
        //                if (Userlist.Count > 0)
        //                {
        //                    foreach (var user in Userlist)
        //                    {
        //                        ModelRoleMember emrm = new WindowsModelRoleMember();
        //                        emrm.MemberName = "" + ConfigurationManager.AppSettings["TabularAreaName"].ToString() + "\\" + user + "";
        //                        mr.Members.Add(emrm);
        //                    }
        //                }

        //                asDatabase.Model.Roles.Add(mr);
        //            }
        //            asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);

        //        }
        //    }
        //}


        ///// <summary>
        ///// 设置列权限 （角色存在）
        ///// </summary>
        ///// <param name="RoleName"></param>
        ///// <param name="TableList"></param>
        //public void SetColumnPermissionExist(string RoleName, List<ColumnPermissionVM> ColumnData)
        //{
        //    using (Server server = new Server())
        //    {
        //        server.Connect(ConnectionString);
        //        Database asDatabase;
        //        if (server.Databases.FindByName(DataBases) != null)
        //        {
        //            asDatabase = server.Databases.FindByName(DataBases);
        //            //var db = server.Databases.GetByName(DataBases);
        //            var Roles = asDatabase.Model.Roles;
        //            var thisRole = Roles.Find(RoleName);
        //            foreach (ColumnPermissionVM table in ColumnData)
        //            {

        //                if (thisRole.TablePermissions.Find(table.tableName) == null)
        //                {

        //                    TablePermission tpm = new TablePermission();
        //                    tpm.Table = asDatabase.Model.Tables.Find(table.tableName);
        //                    foreach (string column in table.columnData)
        //                    {
        //                        ColumnPermission cpm = new ColumnPermission();
        //                        cpm.Column = tpm.Table.Columns.Find(column);
        //                        cpm.MetadataPermission = MetadataPermission.None;
        //                        tpm.ColumnPermissions.Add(cpm);
        //                    }

        //                    thisRole.TablePermissions.Add(tpm);
        //                }
        //                else
        //                {
        //                    foreach (string column in table.columnData)
        //                    {
        //                        if (thisRole.TablePermissions.Find(table.tableName).ColumnPermissions.Find(column)==null)
        //                        {
        //                            ColumnPermission cpm = new ColumnPermission();
        //                            cpm.Column = thisRole.TablePermissions.Find(table.tableName).Table.Columns.Find(column);
        //                            cpm.MetadataPermission = MetadataPermission.None;
        //                            thisRole.TablePermissions.Find(table.tableName).ColumnPermissions.Add(cpm);
        //                        }
        //                        else
        //                        {
        //                            thisRole.TablePermissions.Find(table.tableName).ColumnPermissions.Find(column).MetadataPermission = MetadataPermission.None;
        //                        }

        //                        asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        //                    }

        //                }

        //                asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
        //            }



        //        }


        //    }
        //}

        ///// <summary>
        ///// 设置列权限 （角色不存在）
        ///// </summary>
        ///// <param name="RoleName"></param>
        ///// <param name="TableList"></param>
        //public void SetColumnPermissionNotExist(string RoleName, string TableName, List<string> ColumnList, List<string> Userlist)
        //{
        //    using (Server server = new Server())
        //    {
        //        server.Connect(ConnectionString);
        //        Database asDatabase;
        //        if (server.Databases.FindByName(DataBases) != null)
        //        {
        //            asDatabase = server.Databases.FindByName(DataBases);
        //            var Roles = asDatabase.Model.Roles;

        //            //添加数据权限
        //            ModelRole mr = new ModelRole();
        //            mr.Name = RoleName;
        //            mr.ModelPermission = ModelPermission.Read;

        //            TablePermission tpm = new TablePermission();
        //            tpm.Table = asDatabase.Model.Tables.Find(TableName);
        //            foreach (var column in ColumnList)
        //            {

        //                ColumnPermission cpm = new ColumnPermission();
        //                cpm.Column = tpm.Table.Columns.Find(column);
        //                cpm.MetadataPermission = MetadataPermission.None;
        //                tpm.ColumnPermissions.Add(cpm);
        //            }

        //            mr.TablePermissions.Add(tpm);
        //            if (Userlist.Count > 0)
        //            {
        //                foreach (var user in Userlist)
        //                {
        //                    ModelRoleMember emrm = new WindowsModelRoleMember();
        //                    emrm.MemberName = "" + ConfigurationManager.AppSettings["TabularAreaName"].ToString() + "\\" + user + "";
        //                    mr.Members.Add(emrm);
        //                }
        //            }

        //            asDatabase.Model.Roles.Add(mr);
        //            asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);

        //        }
        //    }
        //}

        /// <summary>
        /// 删除角色下的用户
        /// </summary>
        /// <param name="Instance"></param>
        /// <param name="DataBases"></param>
        /// <param name="RoleName"></param>
        public void DelUser(string Instance, string DataBases, string RoleName)
        {
            using (Server server = new Server())
            {
                server.Connect("DataSource=" + Instance);
                Database database;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    database = server.Databases.FindByName(DataBases);
                    var roles = database.Model.Roles;
                    var thisrole = roles.Find(RoleName);


                    if (thisrole != null)
                    {
                        var users = thisrole.Members;
                        if (users != null)
                        {
                            foreach (var item in users)
                            {
                                thisrole.Members.Remove(item.MemberName);
                            }
                        }
                        database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                    }
                }
                server.Disconnect();
                server.Dispose();
            }
        }


        /// <summary>
        /// 向角色下面添加成员 
        /// </summary>
        public void AddMember(string Instance, string DataBases, string RoleName, string Account, string DomainAccount)
        {
            using (Server server = new Server())
            {
                server.Connect("DataSource=" + Instance);
                Database database;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    database = server.Databases.FindByName(DataBases);
                    var roles = database.Model.Roles;
                    var thisrole = roles.Find(RoleName);
                    if (thisrole != null)
                    {
                        ModelRoleMember mb = new WindowsModelRoleMember();
                        mb.MemberName = "" + DomainAccount+"\\"+ Account + "";
                        if (!thisrole.Members.ContainsName(mb.MemberName))
                        {
                            thisrole.Members.Add(mb);
                        }
                        database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                        server.Disconnect();
                    }
                    else
                    {
                        ModelRole mr = new ModelRole();
                        mr.Name = RoleName;
                        mr.ModelPermission = ModelPermission.Read;

                        ModelRoleMember emrm = new WindowsModelRoleMember();
                        emrm.MemberName = "" + DomainAccount + "\\"+ Account + "";
                        //mr.Members.Add(emrm);
                        if (!thisrole.Members.ContainsName(emrm.MemberName))
                        {
                            mr.Members.Add(emrm);
                        }
                        //ModelRoleMember emrm11 = new WindowsModelRoleMember();
                        //emrm11.MemberName = "" + Account + "";
                        //mr.Members.Add(emrm11);
                        database.Model.Roles.Add(mr);
                        database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                    }
                }
            }

        }


        /// <summary>
        /// 向角色下面添加成员 
        /// </summary>
        public void AddMemberList(string Instance, string DataBases, string RoleName, List<string> Account, string DomainAccount)
        {
            using (Server server = new Server())
            {
                server.Connect("DataSource=" + Instance);
                Database database;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    database = server.Databases.FindByName(DataBases);
                    var roles = database.Model.Roles;
                    var thisrole = roles.Find(RoleName);
                    if (thisrole != null)
                    {
                        foreach (var user in Account)
                        {
                            ModelRoleMember mb = new WindowsModelRoleMember();
                            mb.MemberName = "" + DomainAccount + "\\" + user + "";
                            if (!thisrole.Members.ContainsName(mb.MemberName))
                            {
                                thisrole.Members.Add(mb);
                            }
                        }
                        
                        
                    }

                    database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                }
            }

        }



        /// <summary>
        /// 删除单个角色
        /// </summary>
        /// <param name="Insert"></param>
        /// <param name="DataBase"></param>
        public void delete_DataBase_Role(string Insert, string DataBase, string RoleName)
        {
            using (Server server = new Server())
            {
                server.Connect("DataSource=" + Insert);
                if (server.Databases.FindByName(DataBase) != null)
                {
                    var database = server.Databases.FindByName(DataBase);
                    var roles = database.Model.Roles;
                    var role = roles.Find(RoleName);
                    if (role != null)
                    {
                        roles.Remove(RoleName);
                    }
                    database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                }
            }
        }


        /// <summary>
        /// 删除数据库下的行权限（单个角色）
        /// </summary>
        /// <param name="Inser"></param>
        /// <param name="DataBase"></param>
        /// <param name="Table"></param>
        public void delelte_table_Column(string Insert, string DataBase, string Table, string RoleName)
        {
            using (Server server = new Server())
            {
                server.Connect("DataSource=" + Insert);
                if (server.Databases.FindByName(DataBase) != null)
                {
                    var database = server.Databases.FindByName(DataBase);
                    var roles = database.Model.Roles.Find(RoleName);

                    if (roles.TablePermissions.Find(Table) != null)
                    {
                        roles.TablePermissions.Find(Table).FilterExpression = "";
                    }
                    database.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                }
            }
        }


        public void setAllTablePermissions()
        {

        }

        /// <summary>
        /// 设置角色的所有表权限,列缺陷都不能看（此角色不存在）
        /// </summary>
        /// <param name="RoleName"></param>
        /// <param name="Userlist"></param>
        /// <param name="DomainAccount"></param>
        public void setAllTablePermissionsNone(string RoleName, List<string> Userlist, string DomainAccount)
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                Database asDatabase;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    asDatabase = server.Databases.FindByName(DataBases);
                    DataTable talbes = getTableDB();
                    ModelRole mr = new ModelRole();
                    mr.Name = RoleName;
                    mr.ModelPermission = ModelPermission.Read;

                    foreach (DataRow item in talbes.Rows)
                    {
                        string AddTable = item["name"].ToString();
                        //var db = server.Databases.GetByName(DataBases);
                        var Roles = asDatabase.Model.Roles;
                        //添加数据权限

                        TablePermission tpm = new TablePermission();
                        tpm.Table = asDatabase.Model.Tables.Find(AddTable);
                        tpm.MetadataPermission = MetadataPermission.None; //表权限设置不能看

                        List<string> Columns = getTableColumnDB(AddTable);
                        foreach (var col in Columns)
                        {
                            string colName = col.ToString();
                            ColumnPermission cpm = new ColumnPermission();

                            cpm.Column = tpm.Table.Columns.Find(colName);
                            cpm.MetadataPermission = MetadataPermission.None;
                            tpm.ColumnPermissions.Add(cpm);
                        }


                        mr.TablePermissions.Add(tpm);


                    }
                    if (Userlist.Count > 0)
                    {
                        foreach (var user in Userlist)
                        {
                            ModelRoleMember emrm = new WindowsModelRoleMember();
                            emrm.MemberName = "" + DomainAccount + "\\" + user + "";
                            mr.Members.Add(emrm);
                        }
                    }
                    asDatabase.Model.Roles.Add(mr);
                    asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);

                }
            }
        }

        /// <summary>
        /// 设置角色的所有表权限,列缺陷都不能看（此角色不存在）
        /// </summary>
        /// <param name="RoleName"></param>
        /// <param name="Userlist"></param>
        /// <param name="DomainAccount"></param>
        public void setAllTablePermissionsNone(string RoleName)
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                Database asDatabase;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    asDatabase = server.Databases.FindByName(DataBases);
                    DataTable talbes = getTableDB();
                    ModelRole mr = new ModelRole();
                    mr.Name = RoleName;
                    mr.ModelPermission = ModelPermission.Read;

                    foreach (DataRow item in talbes.Rows)
                    {
                        string AddTable = item["name"].ToString();
                        //var db = server.Databases.GetByName(DataBases);
                        var Roles = asDatabase.Model.Roles;
                        //添加数据权限

                        TablePermission tpm = new TablePermission();
                        tpm.Table = asDatabase.Model.Tables.Find(AddTable);
                        tpm.MetadataPermission = MetadataPermission.None; //表权限设置不能看

                        List<string> Columns = getTableColumnDB(AddTable);
                        foreach (var col in Columns)
                        {
                            string colName = col.ToString();
                            ColumnPermission cpm = new ColumnPermission();

                            cpm.Column = tpm.Table.Columns.Find(colName);
                            cpm.MetadataPermission = MetadataPermission.None;
                            tpm.ColumnPermissions.Add(cpm);
                        }


                        mr.TablePermissions.Add(tpm);


                    }

                    asDatabase.Model.Roles.Add(mr);
                    asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);

                }
            }
        }


        /// <summary>
        /// 设置角色的所有表权限都能看（此角色不存在）
        /// </summary>
        /// <param name="RoleName"></param>
        /// <param name="Userlist"></param>
        /// <param name="DomainAccount"></param>
        public void setAllTablePermissionsRead(string RoleName)
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                Database asDatabase;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    asDatabase = server.Databases.FindByName(DataBases);
                    DataTable talbes = getTableDB();
                    ModelRole mr = new ModelRole();
                    mr.Name = RoleName;
                    mr.ModelPermission = ModelPermission.Read;

                    foreach (DataRow item in talbes.Rows)
                    {
                        string AddTable = item["name"].ToString();
                        //var db = server.Databases.GetByName(DataBases);
                        var Roles = asDatabase.Model.Roles;
                        //添加数据权限

                        TablePermission tpm = new TablePermission();
                        tpm.Table = asDatabase.Model.Tables.Find(AddTable);
                        tpm.MetadataPermission = MetadataPermission.Read; //表权限设置能看

                        //List<string> Columns = getTableColumnDB(AddTable);
                        //foreach (var col in Columns)
                        //{
                        //    string colName = col.ToString();
                        //    ColumnPermission cpm = new ColumnPermission();

                        //    cpm.Column = tpm.Table.Columns.Find(colName);
                        //    cpm.MetadataPermission = MetadataPermission.None;
                        //    tpm.ColumnPermissions.Add(cpm);
                        //}


                        mr.TablePermissions.Add(tpm);


                    }

                    asDatabase.Model.Roles.Add(mr);
                    asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);

                }
            }
        }

        /// <summary>
        /// 设置表权限 角色不存在
        /// </summary>
        /// <param name="RoleName"></param>
        /// <param name="TableList"></param>
        /// <param name="Userlist"></param>
        /// <param name="DomainAccount"></param>
        public void SetTablePermissionNotExist(string RoleName, List<string> TableList, List<string> Userlist, string DomainAccount)
        {
            using (Server server = new Server())

            {
                server.Connect(ConnectionString);
                Database asDatabase;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    asDatabase = server.Databases.FindByName(DataBases);
                    ModelRole mr = new ModelRole();
                    mr.Name = RoleName;
                    mr.ModelPermission = ModelPermission.Read;

                    DataTable talbes = getTableDB(); //获取当前所有表
                    foreach (DataRow item in talbes.Rows)
                    {
                        string AddTable = item["name"].ToString();

                        TablePermission tpm = new TablePermission();
                        tpm.Table = asDatabase.Model.Tables.Find(AddTable);
                        if (TableList.Exists(e => e == AddTable))
                        {
                            tpm.MetadataPermission = MetadataPermission.Read; //表权限
                        }
                        else //未配置的设置为不可读
                        {
                            tpm.MetadataPermission = MetadataPermission.None; //表权限不可读
                        }
                        mr.TablePermissions.Add(tpm);
                    }

                    //添加用户
                    if (Userlist.Count > 0)
                    {
                        foreach (var user in Userlist)
                        {
                            ModelRoleMember emrm = new WindowsModelRoleMember();
                            emrm.MemberName = "" + DomainAccount + "\\" + user + "";
                            if (!mr.Members.Contains(emrm.MemberName))
                            {
                                mr.Members.Add(emrm);
                            }

                        }
                    }

                    asDatabase.Model.Roles.Add(mr);
                    asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);

                }
            }
        }


        /// <summary>
        /// 设置列权限 （角色存在）
        /// </summary>
        /// <param name="RoleName"></param>
        /// <param name="TableList"></param>
        public void SetColumnPermissionExist(string RoleName, string tableName, List<string> columnData)
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                Database asDatabase;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    asDatabase = server.Databases.FindByName(DataBases);
                    //var db = server.Databases.GetByName(DataBases);
                    var Roles = asDatabase.Model.Roles;
                    var thisRole = Roles.Find(RoleName);

                    List<string> ColumnAlllist = getTableColumnDB(tableName);
                    if (thisRole.TablePermissions.Find(tableName) == null)
                    {

                        TablePermission tpm = new TablePermission();
                        tpm.Table = asDatabase.Model.Tables.Find(tableName);


                        foreach (var column in ColumnAlllist)
                        {   //将为配置的列权限设置为none 不能读取
                            if (columnData.Exists(e => e == column))
                            {
                                ColumnPermission cpm = new ColumnPermission();
                                cpm.Column = tpm.Table.Columns.Find(column);
                                if (cpm.Column != null)
                                {
                                    cpm.MetadataPermission = MetadataPermission.Read;
                                    tpm.ColumnPermissions.Add(cpm);
                                }
                            }
                            else
                            {
                                ColumnPermission cpm = new ColumnPermission();
                                cpm.Column = tpm.Table.Columns.Find(column);
                                if (cpm.Column != null)
                                {
                                    cpm.MetadataPermission = MetadataPermission.None;
                                    tpm.ColumnPermissions.Add(cpm);
                                }
                                
                            }
                        }
                        //foreach (string column in table.columnData)
                        //{
                        //    ColumnPermission cpm = new ColumnPermission();
                        //    cpm.Column = tpm.Table.Columns.Find(column);
                        //    cpm.MetadataPermission = MetadataPermission.Read;
                        //    tpm.ColumnPermissions.Add(cpm);
                        //}

                        thisRole.TablePermissions.Add(tpm);
                    }
                    else
                    {
                        foreach (var column in ColumnAlllist)
                        {   //将未配置的列权限设置为none 不能读取
                            if (columnData.Exists(e => e == column))
                            {
                                if (thisRole.TablePermissions.Find(tableName).ColumnPermissions.Find(column) == null)
                                {

                                    ColumnPermission cpm = new ColumnPermission();
                                    cpm.Column = thisRole.TablePermissions.Find(tableName).Table.Columns.Find(column);
                                    cpm.MetadataPermission = MetadataPermission.Read;
                                    thisRole.TablePermissions.Find(tableName).ColumnPermissions.Add(cpm);
                                }
                                else
                                {
                                    thisRole.TablePermissions.Find(tableName).ColumnPermissions.Find(column).MetadataPermission = MetadataPermission.Read;
                                }

                                //asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                            }
                            else //设置其余的列权限不能读取
                            {
                                if (thisRole.TablePermissions.Find(tableName).ColumnPermissions.Find(column) == null)
                                {
                                    ColumnPermission cpm = new ColumnPermission();
                                    cpm.Column = thisRole.TablePermissions.Find(tableName).Table.Columns.Find(column);
                                    cpm.MetadataPermission = MetadataPermission.None;
                                    thisRole.TablePermissions.Find(tableName).ColumnPermissions.Add(cpm);
                                }
                                else
                                {
                                    thisRole.TablePermissions.Find(tableName).ColumnPermissions.Find(column).MetadataPermission = MetadataPermission.None;
                                }

                                

                            }

                        }

                        asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                        //    foreach (string column in table.columnData)
                        //{

                        //}

                    }
                }
            }
        }

        /// <summary>
        /// 设置列权限 （角色不存在）
        /// </summary>
        /// <param name="RoleName"></param>
        /// <param name="TableList"></param>
        public void SetColumnPermissionNotExist(string RoleName, string TableName, List<string> ColumnList, List<string> Userlist, string DomainAccount)
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                Database asDatabase;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    asDatabase = server.Databases.FindByName(DataBases);
                    var Roles = asDatabase.Model.Roles;

                    //添加数据权限
                    ModelRole mr = new ModelRole();
                    mr.Name = RoleName;
                    mr.ModelPermission = ModelPermission.Read;

                    TablePermission tpm = new TablePermission();
                    tpm.Table = asDatabase.Model.Tables.Find(TableName);
                    if (tpm.Table != null)
                    {
                        List<string> ColumnAlllist = getTableColumnDB(TableName);
                        foreach (var column in ColumnAlllist)
                        {
                            if (ColumnList.Exists(e => e == column))
                            {
                                ColumnPermission cpm = new ColumnPermission();
                                cpm.Column = tpm.Table.Columns.Find(column);
                                cpm.MetadataPermission = MetadataPermission.Read;
                                tpm.ColumnPermissions.Add(cpm);
                            }
                            else
                            {
                                ColumnPermission cpm = new ColumnPermission();
                                cpm.Column = tpm.Table.Columns.Find(column);
                                cpm.MetadataPermission = MetadataPermission.None;
                                tpm.ColumnPermissions.Add(cpm);
                            }
                        }
                    }

                    mr.TablePermissions.Add(tpm);
                    if (Userlist.Count > 0)
                    {
                        foreach (var user in Userlist)
                        {
                            ModelRoleMember emrm = new WindowsModelRoleMember();
                            emrm.MemberName = "" + DomainAccount + "\\" + user + "";
                            //if (!mr.Members.Contains(emrm.MemberName))
                            //{
                                mr.Members.Add(emrm);
                            //}
                            //mr.Members.Add(emrm);
                        }
                    }

                    asDatabase.Model.Roles.Add(mr);
                    asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);

                }
            }
        }


        /// <summary>
        /// 设置列一张表下的所有行可读 （角色存在）
        /// </summary>
        /// <param name="RoleName"></param>
        public void setAllColumnPermissionRead(string RoleName, string tableName)
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                Database asDatabase;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    asDatabase = server.Databases.FindByName(DataBases);
                    ModelRole thisRole = asDatabase.Model.Roles.Find(RoleName);
                    thisRole.TablePermissions.Find(tableName).FilterExpression = "";
                    asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                }
            }
        }

        /// <summary>
        /// 设置列一张表下的所有列可读
        /// </summary>
        /// <param name="RoleName"></param>
        public void setAllColumnPermissionsRead(string RoleName, string tableName)
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                Database asDatabase;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    asDatabase = server.Databases.FindByName(DataBases);
                    DataTable talbes = getTableDB();
                    bool HasRole = RoleIsHave(RoleName);
                    ModelRole thisRole = asDatabase.Model.Roles.Find(RoleName);

                    //List<string> Columns = getTableColumnDB(tableName);
                    if (thisRole.TablePermissions.Find(tableName) == null)
                    {

                        TablePermission tpm = new TablePermission();
                        tpm.Table = asDatabase.Model.Tables.Find(tableName);
                        tpm.MetadataPermission = MetadataPermission.Read;
                        //foreach (string column in Columns)
                        //{
                        //    ColumnPermission cpm = new ColumnPermission();
                        //    cpm.Column = tpm.Table.Columns.Find(column);
                        //    cpm.MetadataPermission = MetadataPermission.Read;
                        //    tpm.ColumnPermissions.Add(cpm);
                        //}

                        thisRole.TablePermissions.Add(tpm);
                    }
                    else
                    {
                        thisRole.TablePermissions.Find(tableName).ColumnPermissions.Clear();
                    }
                    asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);


                }
            }
        }


        /// <summary>
        /// 获取表下面的列
        /// </summary>
        public List<string> getTableColumnDB(string table)
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                var db = server.Databases.GetByName(DataBases);
                List<string> dtResult = new List<string>();
                var tb = db.Model.Tables.Find(table);
                if (tb==null)
                {
                    return dtResult;
                }
                var Columns = db.Model.Tables[table].Columns;
                //DataTable dtResult = new DataTable();
                
                //dtResult.Columns.Add("name");
                for (int a = 0; a < Columns.Count; a++)
                {
                    string name = Columns[a].Name;
                    int b = name.IndexOf("RowNumber");
                    if (name.IndexOf("RowNumber") < 0)
                    {
                        dtResult.Add(Columns[a].Name);
                    }

                }
                //DataView dv = new DataView(dtResult);
                //DataTable distdt = dv.ToTable(true, "name");
                return dtResult;
            }
        }



        /// <summary>
        /// 表格模型设置行权限(角色不存在)  2019年8月26日13:51:23  Dennyhui
        /// </summary>
        public void TabularSetRolePermissionRoleDoesNotExist(List<string> Data, string tableName, string field, string RoleName, List<string> Userlist, string DomainAccount)
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                Database asDatabase;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    asDatabase = server.Databases.FindByName(DataBases);
                    //var db = server.Databases.GetByName(DataBases);
                    var Roles = asDatabase.Model.Roles;
                    //添加数据权限dddd

                    TablePermission tpm = new TablePermission();
                    tpm.Table = asDatabase.Model.Tables.Find(tableName);
                    //tpm.MetadataPermission = MetadataPermission.None; //表权限


                    //ColumnPermission cpm = new ColumnPermission();
                    //cpm.Column = tpm.Table.Columns.Find("ID_Date");
                    //cpm.MetadataPermission = MetadataPermission.None;
                    //tpm.ColumnPermissions.Add(cpm);

                    StringBuilder filterStr = new StringBuilder();
                    for (int i = 0, ListCount = Data.Count; i < ListCount; i++)
                    {
                        string str = IsNumeric(Data[i]);
                        string stastr = str == "" ? "VALUE(" : "";
                        string Endstr = str == "" ? ")" : "";
                        if (i == ListCount - 1)
                        {
                            filterStr.Append("" + stastr + " " + tableName + "[" + field + "] " + Endstr + "= " + str + "" + Data[i] + "" + str + "  ");
                        }
                        else
                        {
                            filterStr.Append("" + stastr + " " + tableName + "[" + field + "] " + Endstr + " = " + str + "" + Data[i] + "" + str + " || ");
                        }
                    }

                    tpm.FilterExpression = filterStr.ToString();
                    ModelRole mr = new ModelRole();
                    mr.Name = RoleName;
                    mr.ModelPermission = ModelPermission.Read;
                    mr.TablePermissions.Add(tpm);
                    //添加用户
                    foreach (var user in Userlist)
                    {
                        ModelRoleMember emrm = new WindowsModelRoleMember();
                        emrm.MemberName = "" + DomainAccount + "\\" + user + "";
                        mr.Members.Add(emrm);
                    }
                    asDatabase.Model.Roles.Add(mr);
                    asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                }
            }
        }


        /// <summary>
        /// 表格模型设置权限(角色存在)  2019年8月26日13:51:23  Dennyhui
        /// </summary>
        public void TabularSetRolePermissionRoleExist(string RoleName, string tableName, List<Tabular_Role_mapping> TSAlist)
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                Database asDatabase;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    asDatabase = server.Databases.FindByName(DataBases);
                    //var db = server.Databases.GetByName(DataBases);
                    var Roles = asDatabase.Model.Roles;
                    var thisRole = Roles.Find(RoleName);

                    StringBuilder filterStr = new StringBuilder();
                    if (TSAlist.Count > 0)
                    {
                        var OldListcount = TSAlist.GroupBy(e => e.Filed).Select(g => new Filder_DataClass
                        {
                            Filder = g.FirstOrDefault().Filed,
                            Data = g.Select(e => e.Data).ToList()
                        }).ToList();
                        var FieleCoun = OldListcount.Count();

                        if (OldListcount.Count() > 1)
                        {
                            filterStr.Append("AND(");
                            for (int i = 0, ListCount = OldListcount[0].Data.Count; i < ListCount; i++)
                            {
                                string str = IsNumeric(OldListcount[0].Data[i]);
                                string stastr = str == "" ? "VALUE(" : "";
                                string Endstr = str == "" ? ")" : "";
                                if (i == ListCount - 1)
                                {

                                    filterStr.Append("" + stastr + " [" + OldListcount[0].Filder + "] " + Endstr + " = " + str + "" + OldListcount[0].Data[i] + "" + str + " ");

                                }
                                else
                                {
                                    filterStr.Append("" + stastr + " [" + OldListcount[0].Filder + "] " + Endstr + " = " + str + "" + OldListcount[0].Data[i] + "" + str + " || ");
                                }
                            }
                            OldListcount.RemoveAt(0);
                            filterStr.Append(TraversalDax(OldListcount));
                            var FilderCount = FieleCoun;
                            int SupportCount = 0;
                            SupportCount = FilderCount - 1;
                            for (int i = 0; i < SupportCount; i++)
                            {
                                filterStr.Append(" )");
                            }

                        }
                        else if (OldListcount.Count() == 1)
                        {
                            for (int i = 0, ListCount = OldListcount[0].Data.Count; i < ListCount; i++)
                            {
                                string str = IsNumeric(OldListcount[0].Data[i]);
                                string stastr = str == "" ? "VALUE(" : "";
                                string Endstr = str == "" ? ")" : "";
                                if (i == ListCount - 1)
                                {
                                    filterStr.Append("" + stastr + " " + Table + "[" + OldListcount[0].Filder + "] " + Endstr + " = " + str + "" + OldListcount[0].Data[i] + "" + str + " ");
                                }
                                else
                                {
                                    filterStr.Append("" + stastr + " " + Table + "[" + OldListcount[0].Filder + "] " + Endstr + " = " + str + "" + OldListcount[0].Data[i] + "" + str + " || ");
                                }
                            }
                        }

                    }
                    else
                    {
                        return;
                    }


                    if (thisRole.TablePermissions.Find(tableName) == null)
                    {
                        TablePermission tpm = new TablePermission();
                        tpm.Table = asDatabase.Model.Tables.Find(tableName);

                        tpm.FilterExpression = filterStr.ToString();
                        thisRole.TablePermissions.Add(tpm);
                    }
                    else
                    {
                        thisRole.TablePermissions.Find(tableName).FilterExpression = filterStr.ToString();
                    }
                    asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                }
            }
        }

        /// <summary>
        /// 通过递归拼接DAX语句
        /// </summary>
        /// <param name="OldListcount"></param>
        /// <returns></returns>
        public string TraversalDax(List<Filder_DataClass> OldListcount)
        {
            StringBuilder sbdax = new StringBuilder();
            sbdax.Append(" , ");
            if (OldListcount.Count() == 1)
            {
                var AList = OldListcount[0];
                for (int i = 0, ListCount = AList.Data.Count(); i < ListCount; i++)
                {
                    string str = IsNumeric(AList.Data[i]);
                    string stastr = str == "" ? "VALUE(" : "";
                    string Endstr = str == "" ? ")" : "";
                    if (i == ListCount - 1)
                    {
                        sbdax.Append("" + stastr + " [" + AList.Filder + "] " + Endstr + "= " + str + "" + AList.Data[i] + "" + str + " ");
                    }
                    else
                    {
                        sbdax.Append("" + stastr + " [" + AList.Filder + "] " + Endstr + "= " + str + "" + AList.Data[i] + "" + str + " || ");
                    }
                }
                OldListcount.RemoveRange(0, 1);
            }
            else
            {
                var AList = OldListcount[0];
                sbdax.Append(" AND( ");
                for (int i = 0, ListCount = AList.Data.Count(); i < ListCount; i++)
                {
                    string str = IsNumeric(AList.Data[i]);
                    string stastr = str == "" ? "VALUE(" : "";
                    string Endstr = str == "" ? ")" : "";
                    if (i == ListCount - 1)
                    {
                        sbdax.Append("" + stastr + " [" + AList.Filder + "] " + Endstr + "= " + str + "" + AList.Data[i] + "" + str + " ");
                    }
                    else
                    {
                        sbdax.Append("" + stastr + " [" + AList.Filder + "] " + Endstr + "= " + str + "" + AList.Data[i] + "" + str + " || ");
                    }
                }

                OldListcount.RemoveRange(0, 1);
                if (OldListcount.Count() >= 2)
                {
                    sbdax.Append(" , AND( ");
                }
                else
                {
                    sbdax.Append(" , ");

                }
                var AList1 = OldListcount[0];
                for (int i = 0, ListCount = AList1.Data.Count(); i < ListCount; i++)
                {
                    string str = IsNumeric(AList1.Data[i]);
                    string stastr = str == "" ? "VALUE(" : "";
                    string Endstr = str == "" ? ")" : "";
                    if (i == ListCount - 1)
                    {
                        sbdax.Append("" + stastr + " [" + AList1.Filder + "] " + Endstr + "= " + str + "" + AList1.Data[i] + "" + str + " ");
                    }
                    else
                    {
                        sbdax.Append("" + stastr + " [" + AList1.Filder + "] " + Endstr + "= " + str + "" + AList1.Data[i] + "" + str + " || ");
                    }
                }
                OldListcount.RemoveRange(0, 1);
            }
            if (OldListcount.Count > 0)
            {
                sbdax.Append(TraversalDax(OldListcount));
            }
            return sbdax.ToString();
        }

        public void AddRole(string roleName)
        {
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);
                Database asDatabase;
                if (server.Databases.FindByName(DataBases) != null)
                {
                    asDatabase = server.Databases.FindByName(DataBases);
                    ModelRole mr = new ModelRole();
                    mr.Name = roleName;
                    mr.ModelPermission = ModelPermission.Read;
                    asDatabase.Model.Roles.Add(mr);
                    asDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                }
            }
        }
    }

}
