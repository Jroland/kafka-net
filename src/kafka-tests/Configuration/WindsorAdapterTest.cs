using Castle.Windsor;
using KafkaNet.Configuration;
using KafkaNet.Windsor;
using NUnit.Framework;

namespace kafka_tests.Configuration
{
    [TestFixture]
    public class WindsorAdapterTest
    {
        private WindsorAdapter _defaultContainer;

        [SetUp]
        public void SetUp()
        {
            _defaultContainer = new WindsorAdapter(new WindsorContainer());
        }

        [Test]
        public void FirstRegistrationShouldWin()
        {
            var oneService = new OneService();
            var anotherService = new AnotherService();
            _defaultContainer.Register<IService>(_ => oneService);
            _defaultContainer.Register<IService>(_ => anotherService);
            Assert.AreEqual(oneService, _defaultContainer.Resolve<IService>());
        }

        [Test]
        [ExpectedException(typeof (ServiceNotFound))]
        public void ShouldThrowExceptionIfServiceNotRegistered()
        {
            _defaultContainer.Resolve<IService>();
        }

        [Test]
        public void ShouldRememberCreatedInstance()
        {
            _defaultContainer.Register<IService>(_ => new OneService());
            var instance = _defaultContainer.Resolve<IService>();
            Assert.AreSame(instance, _defaultContainer.Resolve<IService>());
        }
    }
}