using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using AzureCostAnalytics.Core.Services;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(services =>
    {
        services.AddScoped<ICostDataService, CostDataService>();
    })
    .Build();

host.Run();
