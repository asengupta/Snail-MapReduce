require 'rubygems'
require 'matrix'
require './matrix_block_mixin'
require './map_reduce'

class Matrix
	def each_column
		self.column_vectors.each {|column_vector| yield(column_vector)}
	end
	def each_row
		self.row_vectors.each {|row_vector| yield(row_vector)}
	end
end

def pair_up(key, value)
	inputs = []
	a = value[:a]
	b = value[:b]

	row = 0
	a.each_column do |a_column|
		column = 0
		b.row_vectors[row].each do |e|
			inputs << {:key => column.to_s, :value => {:scalar => e, :vector => a_column}}
			column += 1
		end
		row += 1
	end
	inputs
end

def multiply(key, value)
	[{ :key => key, :value => value[:vector].collect {|vc| vc * value[:scalar]}}]
end

def add_reduce(key, values)
	empty = Array.new(values.first.size)
	empty.fill(0)
	v = values.inject(Vector.elements(empty)) {|sum, v| sum + v}
	[{:key => 'X', :value => v}]
end

def append_reduce(key, values)
	[{:key => 'result', :value => Matrix.columns(values)}]
end

def m(order)
	Matrix.build(order, order) {|row, col| rand(20) }
end

order = 8
mappings = reductions = (Math.log2(order) - 1).to_i
m1 = m(order)
m2 = m(order)

operations = []
operations << Mapper.new {|k,v| pair_up(k,v)}
operations << Mapper.new {|k,v| multiply(k,v)}
operations << Reducer.new {|k,vs| add_reduce(k,vs)}
operations << Reducer.new {|k,vs| append_reduce(k,vs)}

result = MapReduceRunner.new(operations).run([{:key => "X", :value => {:a => m1, :b => m2}}])

puts m1*m2 == result[0][:value]

