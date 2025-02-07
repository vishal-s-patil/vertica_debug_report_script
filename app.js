import React, { useState } from "react";

const App = () => {
  const [data, setData] = useState({
    delete_vectors: [{ message: "No outliers in delete vector count", status: "OK" }],
    error_messages: [{ message: "No Errors Found", status: "OK" }],
    long_running_queries: [{ message: "No long running queries.", status: "OK" }],
    query_count: [{ message: "total 4229 queries by 4 users in past 3.0 hours", status: "OK" }],
    resource_queues: [{ message: "No Queries in Queue", status: "OK" }],
    sessions: [{ message: "No Active Queries", status: "OK" }]
  });

  const fetchData = async () => {
    try {
      const response = await fetch("http://localhost:5500/globalrefresh?subcluster_name=secondary_subcluster_1");
      const result = await response.json();
      setData(result);
    } catch (error) {
      console.error("Error fetching data:", error);
    }
  };

  // Convert JSON object into an array of rows
  const tableData = Object.entries(data).flatMap(([query_name, records]) =>
    records.map(record => ({
      query_name,
      status: record.status,
      message: record.message
    }))
  );

  return (
    <div className="p-6">
      <h2 className="text-xl font-bold mb-4">Query Status Table</h2>
      <button 
        onClick={fetchData} 
        className="mb-4 px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600">
        Refresh
      </button>
      <table className="w-full border border-gray-300">
        <thead>
          <tr className="bg-gray-200">
            <th className="border px-4 py-2">Query Name</th>
            <th className="border px-4 py-2">Status</th>
            <th className="border px-4 py-2">Message</th>
          </tr>
        </thead>
        <tbody>
          {tableData.map((row, index) => (
            <tr key={index} className="border-b">
              <td className="border px-4 py-2">{row.query_name}</td>
              <td className="border px-4 py-2">{row.status}</td>
              <td className="border px-4 py-2">{row.message}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default App;
