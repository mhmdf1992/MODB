using System.Collections.Concurrent;
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

        public IDB Get(string name, bool generateIfNotExists = true)
        {
            name.IsValidDBName();
            if(_dbs.ContainsKey(name))
                return _dbs[name];
            if(generateIfNotExists){
                _dbs.TryAdd(name, new DB(Path.Combine(_path, $"{name}.db")));
                return _dbs[name];
            }
            throw new Exceptions.DBNotFoundException(name);
        }
    }
}