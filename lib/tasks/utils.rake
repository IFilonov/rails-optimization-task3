# Наивная загрузка данных из json-файла в БД
# rake reload_json[fixtures/small.json]
task :old_reload_json, [:file_name] => :environment do |_task, args|

  start_time = Time.now.to_i
  json = JSON.parse(File.read(args.file_name))

  ActiveRecord::Base.transaction do
    City.delete_all
    Bus.delete_all
    Service.delete_all
    Trip.delete_all
    ActiveRecord::Base.connection.execute('delete from buses_services;')

    json.each do |trip|
      from = City.find_or_create_by(name: trip['from'])
      to = City.find_or_create_by(name: trip['to'])
      services = []
      trip['bus']['services'].each do |service|
        s = Service.find_or_create_by(name: service)
        services << s
      end
      bus = Bus.find_or_create_by(number: trip['bus']['number'])
      bus.update(model: trip['bus']['model'], services: services)

      Trip.create!(
        from: from,
        to: to,
        bus: bus,
        start_time: trip['start_time'],
        duration_minutes: trip['duration_minutes'],
        price_cents: trip['price_cents'],
      )
    end
  end
  puts "Imported: "
  puts " - Cities: #{City.count}"
  puts " - Services: #{Service.count}"
  puts " - Buses: #{Bus.count}"
  puts " - BusesServices: #{BusesService.count}"
  puts " - Trips: #{Trip.count}"
  puts
  puts "************* #{Time.now.to_i - start_time} sec; *************"
end

task :reload_json, [:file_name] => :environment do |_task, args|

  at_exit do
    puts "MEMORY USAGE: %d MB" % (`ps -o rss= -p #{Process.pid}`.to_i / 1024)
  end

  start_time = Time.now.to_i
  json = JSON.parse(File.read(args.file_name))
  benchmark = Benchmark.bm(20) do |bm|
    ActiveRecord::Base.transaction do
      City.delete_all
      Bus.delete_all
      Service.delete_all
      Trip.delete_all
      ActiveRecord::Base.connection.execute('delete from buses_services;')

      cities = {}
      services = {}
      trips = []
      buses = {}

      bm.report('Json parsing') do
        pb = ProgressBar.new(json.length)
        json.each do |trip|
          trip_from = trip['from']
          trip_to = trip['to']
          cities[trip_from] = City.new(name: trip_from) if cities[trip_from].nil?
          cities[trip_to] = City.new(name: trip_to) if cities[trip_to].nil?

          bus_number = trip['bus']['number']
          if buses[bus_number].nil?
            buses[bus_number] = Bus.new(number: bus_number, model: trip['bus']['model'])

            trip['bus']['services'].each do |service|
              services[service] = Service.new(name: service) if services[service].nil?
              buses[bus_number].buses_services.build(service: services[service])
            end
          end

          trips << Trip.new(
            from: cities[trip_from],
            to: cities[trip_to],
            bus: buses[bus_number],
            start_time: trip['start_time'],
            duration_minutes: trip['duration_minutes'],
            price_cents: trip['price_cents'],
            )
          pb.increment!
        end
      end
      bm.report('Database loading') do
        City.import cities.values
        Service.import services.values
        Bus.import buses.values, recursive: true, batch_size: 5000
        Trip.import trips, batch_size: 10000
      end
    end
  end
  puts "************* #{Time.now.to_i - start_time} sec; *************"
  puts "Imported: "
  puts " - Cities: #{City.count}"
  puts " - Services: #{Service.count}"
  puts " - Buses: #{Bus.count}"
  puts " - BusesServices: #{BusesService.count}"
  puts " - Trips: #{Trip.count}"
  puts

  puts "Benchmark:"
  puts " - #{benchmark.sum}"
  puts
end