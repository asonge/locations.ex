defmodule Locations do
  def process do
    files = Path.wildcard("raw/*")
    Enum.map(files, fn file ->
      Task.async(Locations, :process_file, [file])
    end)
    |> Enum.each fn(task) ->
      Task.await(task, :infinity)
    end
  end

  def process_file(file) do
    IO.puts "Reading #{file}"
    locations = File.stream!(file)
    |> CSV.decode(separator: ?\t)
    |> Enum.map fn location ->
      
      keys = [
        :country_code,
        :postal_code,
        :place_name,
        :admin_name_1,
        :admin_code_1,
        :admin_name_2,
        :admin_code_2,
        :admin_name_3,
        :admin_code_3,
        :latitude,
        :longitude,
        :accuracy
      ]

      location = Enum.zip(keys, location)

      parse_coord = fn (string) ->
        { float, _ } = Float.parse(string)
        float
      end
    
      location = %{
        location_name: location[:place_name],
        postal_code: location[:postal_code],
        region_short: location[:admin_name_1],
        region_name: location[:admin_name_1],
        latitude: parse_coord.(location[:latitude]),
        longitude: parse_coord.(location[:longitude]),
        type: "SUB",
        display_name: "",
        slug: "",
        advert_count: 0
       }
    end

    File.mkdir_p("processed")
    path = Path.join("processed", Path.basename(file, ".txt") <> ".json")
    data = locations |> Poison.encode!

    File.write!(path, data)
    :done
  end

  def await(tasks) do
    receive do
      message ->
        case Task.find(tasks, message) do
          { reply, task} ->
            IO.inspect(reply)
            { reply, List.delete(tasks, task) }
          nil ->
            await(tasks)
        end
    end
  end
end
