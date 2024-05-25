using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace MO.MODB{
    public class DBCollection : IDBCollection
    {
        protected readonly ConcurrentDictionary<string, IDB> _dbs;
        protected string _path;
        public DBCollection(string path){
            _dbs = new ConcurrentDictionary<string, IDB>();
            _path = path;
            if(!Directory.Exists(path))
                Directory.CreateDirectory(path);
            LoadDBs();
        }
        protected void LoadDBs(){
            var dirs = Directory.GetDirectories(_path, "*.db");
            if(dirs == null || !dirs.Any())
                return;
            foreach(var dir in dirs){
                var name = Path.GetFileNameWithoutExtension(dir);
                _dbs.TryAdd(name, new DB(dir));
            }
        }
        public bool Exists(string name) => _dbs.ContainsKey(name);

        public IDB Get(string name, bool generateIfNotExists = false)
        {
            name.IsValidDBName();
            var dbname = name.ToLower();
            if(_dbs.ContainsKey(dbname))
                return _dbs[dbname];
            if(generateIfNotExists){
                _dbs.TryAdd(dbname, new DB(Path.Combine(_path, $"{dbname}.db")));
                return _dbs[dbname];
            }
            throw new Exceptions.DBNotFoundException(name);
        }

        public IEnumerable<IDB> All() => _dbs.Values;

        public void Delete(string name)
        {
            var db = Get(name);
            db.Delete();
            _dbs.TryRemove(db.Name, out IDB dbout);
        }
    }
}