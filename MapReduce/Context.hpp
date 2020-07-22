#pragma once
#include <algorithm>
#include "Table.hpp"

template<typename MapKeyType, typename MapValueType, typename ReduceKeyType, typename ReduceValueType>
class Context
{
public:
	using MapContextType = std::vector<std::pair<MapKeyType, MapValueType>>;
	using ReduceContextType = std::vector<std::pair<ReduceKeyType, ReduceValueType>>;
	
	
	void write(MapKeyType key, MapValueType value)
	{
		map_context.emplace_back(key, value);
	}

	void write(ReduceKeyType key, ReduceValueType value)
	{
		reduce_context.emplace_back(key, value);
	}

	auto get_map_context()
	{
		return map_context;
	}

	auto get_reduce_context()
	{
		return reduce_context;
	}

private:
	MapContextType map_context;
	ReduceContextType reduce_context;
};

