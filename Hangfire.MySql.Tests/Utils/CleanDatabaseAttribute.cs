using System.Reflection;
using System.Threading;
using System.Transactions;
using Xunit;
using Xunit.Sdk;

namespace Hangfire.MySql.Tests
{
    public class CleanDatabaseAttribute : BeforeAfterTestAttribute
    {
        private static readonly object GlobalLock = new object();
        
        private readonly IsolationLevel _isolationLevel;

        private TransactionScope _transaction;
        
        public CleanDatabaseAttribute(
            IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
        {
            _isolationLevel = isolationLevel;
        }

        public override void Before(MethodInfo methodUnderTest)
        {
            Monitor.Enter(GlobalLock);

            if (_isolationLevel != IsolationLevel.Unspecified)
            {
                _transaction = new TransactionScope(
                    TransactionScopeOption.RequiresNew,
                    new TransactionOptions { IsolationLevel = _isolationLevel });
            }
        }

        public override void After(MethodInfo methodUnderTest)
        {
            try
            {
                if (_transaction != null)
                {
                    _transaction.Dispose();
                }
            }
            finally
            {
                Monitor.Exit(GlobalLock);
            }   
        }
    }
}
