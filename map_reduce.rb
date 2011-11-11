class Partitioner
	def run(space)
		partitions = {}
		space.each do |i|
			puts i.inspect
			key = i[:key]
			partitions[key] = [] if partitions[key].nil?
			partitions[key] << i[:value]
		end
		partitions
	end
end

class Reducer
	def initialize(&block)
		@function = block
	end

	def run(pairs)
		partitions = Partitioner.new.run(pairs)
		space = []
		partitions.each_pair do |k,v|
			space += @function.call(k,v)
		end
		space
	end
end

class Mapper
	def initialize(&block)
		@function = block
	end

	def run(pairs)
		mapped_pairs = []
		pairs.each do |pair|
			mapped_pairs += @function.call(pair[:key], pair[:value])
		end
		mapped_pairs
	end
end

class MapReduceRunner
	def initialize(operations)
		@operations = operations
	end
	
	def run(pairs)
		results = []
		@operations.each {|operation| pairs = operation.run(pairs)}
		pairs
	end
end

