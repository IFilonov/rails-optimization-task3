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

  puts "************* #{Time.now.to_i - start_time} sec; *************"
end

task :reload_json, [:file_name] => :environment do |_task, args|

  start_time = Time.now.to_i
  json = JSON.parse(File.read(args.file_name))

  ActiveRecord::Base.transaction do
    City.delete_all
    Bus.delete_all
    Service.delete_all
    Trip.delete_all
    ActiveRecord::Base.connection.execute('delete from buses_services;')

    cities = {}
    services = {}
    trips = []
    buses = []

    json.each do |trip|
      trip_from = trip['from']
      trip_to = trip['to']
      cities[trip_from] = City.new(name: trip_from) if cities[trip_from].nil?
      cities[trip_to] = City.new(name: trip_to) if cities[trip_to].nil?

      bus = Bus.new(number: trip['bus']['number'], model: trip['bus']['model'])

      trip['bus']['services'].each do |service|
        services[service] = Service.new(name: service) if services[service].nil?
        bus.buses_services.build(service: services[service])
      end

      buses << bus

      trips << Trip.new(
        from: cities[trip_from],
        to: cities[trip_to],
        bus: buses.last,
        start_time: trip['start_time'],
        duration_minutes: trip['duration_minutes'],
        price_cents: trip['price_cents'],
        )
    end
    City.import cities.values
    Service.import services.values
    Bus.import buses, recursive: true
    Trip.import trips
  end

  puts "************* #{Time.now.to_i - start_time} sec; *************"
end