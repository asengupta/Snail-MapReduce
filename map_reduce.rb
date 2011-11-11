class Partitioner
	def run(space)
		partitions = {}
		space.each do |i|
			key = i[:key]
			partitions[key] = [] if partitions[key].nil?
			partitions[key] << i[:value]
		end
		partitions
	end
end

class Reducer
	def run(partitions)
		space = []
		partitions.each_pair do |k,v|
			space << yield(k,v)
		end
		space
	end
end

class Mapper
	def run(pairs)
		mapped_pairs = []
		pairs.each do |pair|
			mapped_pairs += yield(pair[:key], pair[:value])
		end
		mapped_pairs
	end
end

class MapReduceRunner
	def initialize(mappers, reducers)
		@mappers = mappers
		@reducers = reducers
	end
	
	def run(pairs)
		results = []
		@mappers.each {|mapper| pairs = Mapper.new.run(pairs) {|k,v| mapper.call(k,v)}}
		@reducers.each do |reducer|
			partitions = Partitioner.new.run(pairs)
			pairs = Reducer.new.run(partitions) {|k,v| reducer.call(k,v)}
		end
		pairs
	end
end

