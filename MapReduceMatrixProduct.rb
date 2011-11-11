require 'rubygems'
require 'statsample'
require './matrix_block_mixin'
require './map_reduce'
require 'benchmark'

def map(key, value)
	inputs = []
	a = value[:a]
	b = value[:b]
	if a.row_size == 2
		inputs << {:key=> key, :a => a, :b => b}
		return
	end
	inputs << {:key => key + "00A", :value => {:a => a.block(0,0), :b => b.block(0,0)}}
	inputs << {:key => key + "00B", :value => {:a => a.block(0,1), :b => b.block(1,0)}}

	inputs << {:key => key + "01A", :value => {:a => a.block(0,0), :b => b.block(0,1)}}
	inputs << {:key => key + "01B", :value => {:a => a.block(0,1), :b => b.block(1,1)}}

	inputs << {:key => key + "10A", :value => {:a => a.block(1,0), :b => b.block(0,0)}}
	inputs << {:key => key + "10B", :value => {:a => a.block(1,1), :b => b.block(1,0)}}

	inputs << {:key => key + "11A", :value => {:a => a.block(1,0), :b => b.block(0,1)}}
	inputs << {:key => key + "11B", :value => {:a => a.block(1,1), :b => b.block(1,1)}}

	inputs
end

def join(left_block, right_block)
	rows = []	
	lower_order = left_block.row_size
	lower_order.times do |t|
		rows << left_block.row(t).to_a + right_block.row(t).to_a
	end
	rows
end

def m(order)
	Matrix.build(order, order) {|row, col| rand(20) }
end

def block_join_reduce(key, values)
	p00 = values[values.index {|v| v[:identity] == '00'}][:matrix]
	p01 = values[values.index {|v| v[:identity] == '01'}][:matrix]
	p10 = values[values.index {|v| v[:identity] == '10'}][:matrix]
	p11 = values[values.index {|v| v[:identity] == '11'}][:matrix]
	{:key => key[0..-2], :value => {:identity => key[-1], :matrix => Matrix.rows(join(p00, p01) + join(p10, p11))}}
end

def block_matrix_sum(key, values)
	sum = Matrix.zero(values.first[:matrix].row_size)
	values.each {|m| sum += m[:matrix]}
	{:key => key[0..-3], :value => {:matrix => sum, :identity => key[-2..-1]}}
end

def primitive_map(key, value)
	[{:key => key[0..-2], :value =>  {:matrix => value[:a] * value[:b], :identity => key[0..-2]}}]
end

order = 64
mappings = reductions = (Math.log2(order) - 1).to_i
m1 = m(order)
m2 = m(order)

operations = []
mappings.times do
	operations << Mapper.new {|k,v| map(k,v)}
end

operations << Mapper.new {|k,v| primitive_map(k,v)}

reductions.times do
	operations << Reducer.new {|k,v| block_matrix_sum(k,v)}
	operations << Reducer.new {|k,v| block_join_reduce(k,v)}
end

result = []
mr_time = Benchmark.measure do
	result = MapReduceRunner.new(operations).run([{:key => "X", :value => {:a => m1, :b => m2}}])
end
plain_time = Benchmark.measure do
	m1*m2
end
puts "Unthreaded time = #{plain_time}"
puts "MR time = #{mr_time}"

puts m1*m2 == result[0][:value][:matrix]

