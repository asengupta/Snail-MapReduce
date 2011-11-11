class Matrix
	def block(block_row, block_column)
		raise "Non 2^n matrix" if (row_size & (row_size - 1)) != 0 || (column_size & (column_size - 1)) != 0
		lower_order = row_size/2
		start_row = block_row * lower_order
		start_column = block_column * lower_order
		b = []
		lower_order.times do |r|
			row = []
			lower_order.times do |c|
				row << self[start_row + r, start_column + c]
			end
			b << row
		end
		Matrix.rows(b)
	end
	
	def each_column
		self.column_vectors.each {|column_vector| yield(column_vector)}
	end

	def each_row
		self.row_vectors.each {|row_vector| yield(row_vector)}
	end

end

