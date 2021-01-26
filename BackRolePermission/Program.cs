using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BackRolePermission
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("开始回滚权限");
            BackRolePermissions();
        }
        //public static string connectionString = "Data Source=.\\sqlserver2019; Initial Catalog = WeicaiTest; Integrated Security = SSPI";
        //public static string connectionString = "Data Source=ECISPOWERUATTES;Initial Catalog=Eisai_DMT;User Id=sa;Password=123456;";
        public static string connectionString = "Data Source=ECISPOWERBI;Initial Catalog=Eisai_DMT;User Id=service;Password=fisk@EC1;";
        public static void BackRolePermissions()
        {

            //string Instance = ".\\sqlserver2019";
            //string DataBases = "Eisai_Sales";

            string DomainAccount = "ROOT_EISAI"; //域账号
            string Instance = "ECISPOWERBI";
            string DataBases = "Eisai_BAIM";

            TOMHelper TOM = new TOMHelper(Instance, DataBases);

            TOM.delete_Insert(Instance);

            //string connectionString = "Data Source=ECISPOWERUATTES;Initial Catalog=Eisai_DMT;User Id=sa;Password=123456;";


            DbHelperSQL dbHelper = new DbHelperSQL(connectionString);
            string roleDataSql = $@"SELECT DISTINCT R.RoleID,R.RoleName  FROM 
                                [dbo].[Cfg_Tabular_Role_Mapping] TR LEFT JOIN [dbo].[Cfg_RoleInfo] R ON TR.RoleID =R.RoleID  
                                WHERE (R.Flag='1' OR R.Flag='3') AND R.Validity='1' AND TR.Validity='1' AND R.RoleID IS NOT NULL";
            DataTable roleData = dbHelper.Query(roleDataSql).Tables[0];  //获取需要回滚的角色列表 (flag=1 or 3 )
            string roleAndDataSql = $@"SELECT R.RoleID,R.RoleName,TR.* FROM 
                                [dbo].[Cfg_Tabular_Role_Mapping] TR LEFT JOIN [dbo].[Cfg_RoleInfo] R ON TR.RoleID =R.RoleID 
                                WHERE (R.Flag='1' OR R.Flag='3') AND R.Validity='1' AND TR.Validity='1' AND R.RoleID IS NOT NULL";
            DataTable backRoleData = dbHelper.Query(roleAndDataSql).Tables[0];
            TOMHelper tom = new TOMHelper(Instance, DataBases);

            if (backRoleData.Rows.Count > 0)
            {
                foreach (DataRow role in roleData.Rows)
                {
                    string roleName = role["RoleName"].ToString();
                    string roleID = role["RoleID"].ToString();
                    Console.WriteLine(roleName);

                    //同步表权限
                    Stopwatch sw = new Stopwatch();
                    sw.Start();
                    if (backRoleData.Select($"DataType='Table' AND RoleID='{roleID}'").Count() > 0)
                    {
                        sw.Stop();
                        Console.WriteLine("获取表数据" + sw.Elapsed.TotalSeconds);

                        sw.Reset();
                        sw.Start();
                        var tableAllData = backRoleData.Select($"DataType='Table' AND RoleID='{roleID}'").CopyToDataTable();
                        sw.Stop();
                        Console.WriteLine("从数据筛选是否有全表" + sw.Elapsed.TotalSeconds);
                        sw.Reset();
                        sw.Start();
                        var isAllTable = tableAllData.Select($"DataType='Table' AND Data='All'").FirstOrDefault();
                        sw.Stop();
                        Console.WriteLine("从数据筛选是否有全表" + sw.Elapsed.TotalSeconds);

                        sw.Reset();
                        sw.Start();
                        if (tableAllData.Rows.Count > 0)
                        {
                            var config = tableAllData.Select().FirstOrDefault();

                            if (isAllTable != null)
                            {
                                tom.setAllTablePermissionsRead(roleName);
                            }
                            else
                            {
                                var tablelist = new List<string>();
                                tablelist = (from t in tableAllData.AsEnumerable()
                                             select t.Field<string>("Data")).ToList(); //将这个集合转换成list  
                                //string userDataSql = $"SELECT U.UserAccount FROM  [dbo].[Cfg_Role_User_Mapping] RU LEFT JOIN Cfg_UserInfo  U ON RU.UserID=U.UserInfoID AND U.Validity='1' WHERE RU.Validity='1'AND U.UserInfoID IS NOT NULL  AND RU.RoleID='{roleID}'";
                                //DataTable userData = dbHelper.Query(userDataSql).Tables[0];
                                var userList = new List<string>();
                                //if (userData.Rows.Count > 0)
                                //{
                                //    userList = (from t in userData.AsEnumerable()
                                //                select t.Field<string>("UserAccount")).ToList();
                                //}
                                tom.SetTablePermissionNotExist(roleName, tablelist, userList, DomainAccount);
                            }


                        }
                        sw.Stop();
                        Console.WriteLine("设置表权限" + sw.Elapsed.TotalSeconds);


                    }


                    //列权限设置 
                    if (backRoleData.Select($"DataType='Column' AND RoleID='{roleID}'").Count() > 0) //判断是否存在行权限
                    {

                        Console.WriteLine("============设置列权限");

                        sw.Reset();
                        sw.Start();
                        var columnAllData = backRoleData.Select($"DataType='Column' AND RoleID='{roleID}'").CopyToDataTable();

                        var columnAllConfigData = new List<ColumnListVM>();
                        columnAllConfigData = (from p in columnAllData.AsEnumerable()  //这个list  
                                               select new ColumnListVM
                                               {
                                                   instance = p.Field<string>("Instance"),
                                                   database = p.Field<string>("DataBases"),
                                                   tables = p.Field<string>("Tables")
                                               }).Distinct().ToList(); //将这个集合转换成list 
                        sw.Stop();
                        Console.WriteLine("查出所有列对应的 实例、数据库、表：" + sw.Elapsed.TotalSeconds);
                        if (columnAllData.Rows.Count > 0)
                        {
                            if (columnAllConfigData.Count > 0)
                            {
                                Stopwatch sw1 = new Stopwatch();
                                foreach (ColumnListVM column in columnAllConfigData)
                                {


                                    sw1.Reset();
                                    sw1.Start();
                                    var isAllColumn = columnAllData.Select($"DataType='Column' AND Data='All' AND Instance='{Instance}'  AND DataBases='{DataBases}' AND Tables='{column.tables}'").FirstOrDefault();
                                    sw1.Stop();
                                    Console.WriteLine("查找列权限是否有All数据：" + sw1.Elapsed.TotalSeconds);

                                    if (isAllColumn != null)
                                    {
                                        Console.WriteLine($"设置{column.tables}的列全部可读");
                                        //tom.setAllColumnPermissionsRead(roleName, column.tables);
                                    }
                                    else
                                    {


                                        var columnDatalist = new List<string>();
                                        sw.Reset();
                                        sw.Start();
                                        columnDatalist = (from t in columnAllData.Select($"DataType='Column'  AND Instance='{Instance}'  AND DataBases='{DataBases}' AND Tables='{column.tables}'").AsEnumerable() select t.Field<string>("Data")).ToList(); //将集合转换成list
                                        sw.Stop();
                                        Console.WriteLine($"列权限 查找对应的数据" + sw.Elapsed.TotalSeconds);

                                        if (columnDatalist.Count > 0)
                                        {
                                            List<string> userList = new List<string>();

                                            bool isHasRole = tom.RoleIsHave(roleName);
                                            if (isHasRole)
                                            {
                                                sw.Reset();
                                                sw.Start();
                                                tom.SetColumnPermissionExist(roleName, column.tables, columnDatalist);
                                                sw.Stop();
                                                Console.WriteLine($"设置 {column.tables} 列权限角色存在" + sw.Elapsed.TotalSeconds);
                                            }
                                            else
                                            {
                                                sw.Reset();
                                                sw.Start();
                                                tom.SetColumnPermissionNotExist(roleName, column.tables, columnDatalist, userList, DomainAccount);
                                                sw.Stop();
                                                Console.WriteLine($"设置 {column.tables} 列权限角色不存在" + sw.Elapsed.TotalSeconds);
                                            }
                                        }
                                        else
                                        {
                                            Console.WriteLine("该表未配置列权限");
                                        }


                                        //tom.SetTablePermissionNotExist(roleName, tablelist, userList, DomainAccount);
                                    }

                                }

                            }
                        }

                    }
                    else
                        Console.WriteLine("未设置列权限");
                    //sw.Stop();
                    //Console.WriteLine("设置列权限" + sw.Elapsed.TotalSeconds);


                    //行权限设置
                    if (backRoleData.Select($"DataType='Row' AND RoleID='{roleID}'").Count() > 0)
                    {
                        Console.WriteLine("*********开始设置行权限");
                        var rowAllData = backRoleData.Select($"DataType='Row' AND RoleID='{roleID}'").CopyToDataTable();

                        var rowAllConfigData = new List<RowListVM>();
                        rowAllConfigData = (from p in rowAllData.AsEnumerable()
                                            select new RowListVM
                                            {
                                                instance = p.Field<string>("Instance"),
                                                database = p.Field<string>("DataBases"),
                                                tables = p.Field<string>("Tables")
                                            }).Distinct().ToList(); //将这个集合转换成list 
                        if (rowAllData.Rows.Count > 0)
                        {
                            if (rowAllConfigData.Count > 0)
                            {
                                foreach (RowListVM row in rowAllConfigData)
                                {
                                    Console.WriteLine($"设置 {row.tables} 的行权限");

                                    sw.Reset();
                                    sw.Start();
                                    //查找同一表下的不同列
                                    var fieldList = new List<string>();
                                    fieldList = (from t in rowAllData.Select($"DataType='Row'  AND Instance='{Instance}'  AND DataBases='{DataBases}' AND Tables='{row.tables}'").AsEnumerable() select t.Field<string>("Filed")).ToList(); //将集合转换成list
                                    List<Tabular_Role_mapping> trList = new Program().GetOtherTabularRoleFiledAuthorit(Instance, DataBases, row.tables, roleID);

                                    var isAllRow = rowAllData.Select($"DataType='Row' AND Data='All' AND Instance='{Instance}'  AND DataBases='{DataBases}' AND Tables='{row.tables}' ").FirstOrDefault();
                                    if (isAllRow != null)//设置全部能读，因为角色是还原的所以不做操作
                                    {

                                    }
                                    else
                                    {
                                        var rowDatalist = new List<string>();
                                        rowDatalist = (from t in rowAllData.Select($"DataType='Row'  AND Instance='{Instance}'  AND DataBases='{DataBases}' AND Tables='{row.tables}'").AsEnumerable() select t.Field<string>("Data")).ToList(); //将集合转换成list  

                                        //string userDataSql = $"SELECT U.UserAccount FROM  [dbo].[Cfg_Role_User_Mapping] RU LEFT JOIN Cfg_UserInfo  U ON RU.UserID=U.UserInfoID AND U.Validity='1' WHERE RU.Validity='1'AND U.UserInfoID IS NOT NULL  AND RU.RoleID='{roleID}'";
                                        //DataTable userData = dbHelper.Query(userDataSql).Tables[0];
                                        //List<string> userList = new List<string>();

                                        bool isHasRole = tom.RoleIsHave(roleName);
                                        if (isHasRole)
                                        {
                                            tom.TabularSetRolePermissionRoleExist(roleName, row.tables, trList);
                                            sw.Stop();
                                            Console.WriteLine("还原行权限：" + sw.Elapsed.TotalSeconds);
                                        }
                                        else //角色不存在时
                                        {
                                            //先创建角色
                                            tom.AddRole(roleName);

                                            tom.TabularSetRolePermissionRoleExist(roleName, row.tables, trList);

                                            sw.Stop();
                                            Console.WriteLine("还原行权限：" + sw.Elapsed.TotalSeconds);
                                        }

                                        //tom.SetTablePermissionNotExist(roleName, tablelist, userList, DomainAccount);
                                    }

                                }

                            }
                        }

                    }

                    bool isHasRoles = tom.RoleIsHave(roleName);

                    Console.WriteLine("开始添加人员");
                    if (isHasRoles)
                    {
                        string userDataSqlAll = $"SELECT U.UserAccount FROM  [dbo].[Cfg_Role_User_Mapping] RU LEFT JOIN Cfg_UserInfo  U ON RU.UserID=U.UserInfoID AND U.Validity='1' WHERE RU.Validity='1'AND U.UserInfoID IS NOT NULL  AND RU.RoleID='{roleID}'";
                        DataTable userDataAll = dbHelper.Query(userDataSqlAll).Tables[0];
                        List<string> userListAll = new List<string>();
                        if (userDataAll.Rows.Count > 0)
                        {
                            userListAll = (from t in userDataAll.AsEnumerable()
                                           select t.Field<string>("UserAccount")).ToList();
                        }
                        tom.AddMemberList(Instance, DataBases, roleName, userListAll, DomainAccount);

                    }




                }
            }

            Console.WriteLine("还原结束");
        }

        /// <summary>
        /// 获取一张表配置的所有行权限
        /// </summary>
        /// <param name="Instance"></param>
        /// <param name="DataBase"></param>
        /// <param name="Table"></param>
        /// <param name="RoleId"></param>
        /// <returns></returns>
        public List<Tabular_Role_mapping> GetOtherTabularRoleFiledAuthorit(string Instance, string DataBase, string Table, string RoleId)
        {
            //string connectionString = "Data Source=ECISPOWERUATTES;Initial Catalog=Eisai_DMT;User Id=sa;Password=123456;";
            //string connectionString = "Data Source=ECIdSPOWERUATTES;Initial Catalog=Eisai_DMT;User Id=sa;Password=123456;";
            DbHelperSQL dbHelper = new DbHelperSQL(connectionString);

            List<Tabular_Role_mapping> list = new List<Tabular_Role_mapping>();
            try
            {
                string sql = $@"SELECT * FROM Cfg_Tabular_Role_Mapping t WHERE t.RoleID = '{RoleId}' AND t.Instance = '{Instance}' AND t.DataBases = '{DataBase}' AND t.Tables = '{Table}' AND t.Validity = '1' AND DataType='Row' ";
                DataTable tb = dbHelper.Query(sql).Tables[0];
                if (tb.Rows.Count > 0)
                {
                    list = tb.ToDataList<Tabular_Role_mapping>();
                }

            }
            catch (Exception)
            {

                throw;
            }
            return list;

        }

    }
}
