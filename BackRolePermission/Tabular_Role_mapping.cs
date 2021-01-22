using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BackRolePermission
{
    public class Tabular_Role_mapping
    {
        public int ID { get; set; }
        public string RoleID { get; set; }
        public string Instance { get; set; }
        public string DataBases { get; set; }
        public string Tables { get; set; }
        public string Filed { get; set; }
        public string Data { get; set; }
        public string CreatedUser { get; set; }
        public string UpdatedUser { get; set; }

        public Nullable<System.DateTime> CreatedTime { get; set; }

        public Nullable<System.DateTime> UpdatedTime { get; set; }

    }
}
