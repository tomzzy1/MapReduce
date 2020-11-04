#include <vector>
#include <string>
#include <stack>
#include <algorithm>

class FullReducer
{
public:
	FullReducer(std::vector<std::vector<std::string>> a) : attrs(a) {}
	std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>> build()
	{
		while (attrs.size() != 1)
		{
			int i = 0;
			int consume = 0;
			for (; i < attrs.size(); ++i)
			{
				bool is_ear = false;
				std::vector<int> not_unique;
				for (int j = 0; j < attrs.size(); ++j)
				{
					if (j != i)
						for (int k = 0; k < attrs[i].size(); ++k)
						{
							if (std::find(attrs[j].begin(), attrs[j].end(), attrs[i][k]) != attrs[j].end())
								not_unique.push_back(k);
						}
				}
				for (int j = 0; j < attrs.size(); ++j)
				{
					if (j != i)
					{
						bool all_contain = true;
						for (int k : not_unique)
						{
							if (std::find(attrs[j].begin(), attrs[j].end(), attrs[i][k]) == attrs[j].end())
								all_contain = false;
						}
						if (all_contain)
						{
							is_ear = true;
							consume = j;
						}
					}
				}
				if (is_ear)
					break;
			}
			front.push_back({ attrs[consume], attrs[i] });
			back.push({ attrs[i], attrs[consume] });
			attrs.erase(attrs.begin() + i);
		}
		std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>>
			list(front.begin(), front.end());
		while (!back.empty())
		{
			list.push_back(back.top());
			back.pop();
		}
		return list;
	}
private:
	std::vector<std::vector<std::string>> attrs;
	std::stack<std::pair<std::vector<std::string>, std::vector<std::string>>> back;
	std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>> front;
};