using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MODB{
    public class ActionQueue<T> : ConcurrentQueue<T>
    {
        static object _padlock = new object();
        static object _padlock1 = new object();
        static ActionQueue<T> _queue;
        CancellationTokenSource _cstkn;
        Action<T> _action;
        private ActionQueue(Action<T> action) : base()
        {
            if (action == null)
                throw new Exception("Action not initialized!");
            _action = action;
            _cstkn = new CancellationTokenSource();
        }
        public static ActionQueue<T> GetInstance(Action<T> action = null)
        {
            if(_queue == null)
            {
                lock (_padlock)
                {
                    _queue = new ActionQueue<T>(action);
                }
            }
            return _queue;
        }

        public async Task StartDequeueAsync(int dequeueDelayInMs, Action<Exception, T> onError = null)
        {
            if (_cstkn.IsCancellationRequested)
            {
                lock (_padlock1)
                {
                    _cstkn = new CancellationTokenSource();
                }
            }
            while (!_cstkn.IsCancellationRequested)
            {
                if (TryDequeue(out T obj))
                {
                    try
                    {
                        _action(obj);
                    }catch(Exception ex)
                    {
                        if (onError != null)
                            onError(ex, obj);
                    }
                }
                await Task.Delay(dequeueDelayInMs);
            }
        }

        public void StopDequeue()
        {
            _cstkn.Cancel();
        }
        public string Status => _cstkn.IsCancellationRequested ? "Stopped" : "Started";
    }
}
