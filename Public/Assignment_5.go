// Atef Bader
// Last Edit: 8/25/2021

/////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////
// Use Go lang sql/pq packages to query OnMart database hosted on Postgres/DSCC
// This is a data-intensive and compute-intensive assignment
// How long it takes the complete run of this program?
// It depends on how many CPU cores and logical processors you have on your personal development computer
// And it depends on how many of those cores you want to use, how many Goroutines you want to fork to run concurrently
// It took at least 23 minutes for all requirements to be executed and completed on a computer that has
// Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz, 2592 Mhz, 6 Core(s), 12 Logical Processor(s)
/////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////

// (1) For postgres package

// Documentation URL: https://pkg.go.dev/github.com/lib/pq

// Install: Execute the following command from the terminal window

// go get github.com/lib/pq

/////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////

// (2) For gonum package

// Documentation URL: https://github.com/gonum/plot/wiki/Example-plots

// Install: Execute the following command from the terminal window

//	go get -u gonum.org/v1/gonum/...

/////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////

package main

import (
	"database/sql"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

type OrdersPerCategory struct {
	total        int
	returned     int
	not_returned int
}

type SalesPerCategory struct {
	sales        float64
	profit       float64
	shippingcost float64
}

type WDC_ZipCode struct {
	zipcode                string
	city                   string
	state                  string
	facility_id            string
	distribution_center_id string
	warehouse_id           string
}

// Add your Netid below to connect ot onmart databse on Postgress
const (
	host = "129.105.248.26"
	port = 5432
	//	user   = "ADD_YOUR_netid_HERE"
	user   = "rao1961"
	dbname = "onmart"
)

var (
	zipcode                string
	city                   string
	state                  string
	facility_id            string
	distribution_center_id string
	warehouse_id           string
	deliveryzipcode        string
	count                  string
	sum                    string
)

var zip_codes_mp map[string]WDC_ZipCode

func main() {

	// log the start time for the program
	start := time.Now()

	zip_codes_mp = make(map[string]WDC_ZipCode)

	// How many CPU cores available on your personal development computer?
	// Please make a note of the number physical processors and logical processors
	// Here is what I have on my personal computer: 6 physical cores and 12 logical processors
	// Processor	Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz, 2592 Mhz, 6 Core(s), 12 Logical Processor(s)
	// Golang will give us complete access/control to the logical processors at the application level (Not the OS level)

	// Execute the following command to see how many logical processors available on your personal development computer
	fmt.Println(runtime.NumCPU())

	// Utilize the max num of cores available
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Utilize ONE of cores available
	//runtime.GOMAXPROCS(1)

	// Set the number of CONCURRENT Goroutines considering
	// the number of the Logical Processors available to Golang application

	number_of_concurrent_gophers := runtime.NumCPU() * 4
	// number_of_concurrent_gophers := runtime.NumCPU()
	//number_of_concurrent_gophers := 2

	// Create a channel for synchronization with Goroutines completion
	// a channel is a safe concurrency stream-container that you could use
	// to communicate messages between Goroutines
	// An alternative approach is to use sync.WaitGroup Concurrency Pattern as follows :
	// wg := sync.WaitGroup{} ... wg.Add(1) ... go func() { ... defer wg.Done()... } ... wg.Wait()
	// But lets use channels for synchronizaiton since we want to print Zipcode results of the gopher goroutine
	// from the main goroutine

	// https://gobyexample.com/channels
	// Channels are the pipes that connect concurrent goroutines.
	// You can send values into channels from one goroutine and receive those values into another goroutine.
	ch := make(chan string) // Create a new channel with make(chan val-type). Channels are typed by the values they convey, a string channel in this case.

	// Lets connect to OnMart database hosted on Postgres/DSCC to execute simple queries
	// Make sure you have your VPN connection established before you execute the queries

	// Query 1: Get a list of all zipcodes in logistics_supply_chain_network
	// and store them in a map object keyed on zipcode
	fetch_zipcodes()

	// print the content of zip_codes_mp that got populated by fetch_zipcodes()

	for _, v := range zip_codes_mp {
		fmt.Println("zipcode = ", v.zipcode, "\t city = ", v.city, "\t state = ", v.state, "\t facility_id = ", v.facility_id, "\t distribution_center_id = ", v.distribution_center_id, "\t warehouse_id = ", v.warehouse_id)
	}

	// Query 2: print all zip-code entries in the logistics_supply_chain_network sorted by city in descending order
	// For this query we are forking a goroutine to execute it

	query := "SELECT zipcode, city, state, facility_id, distribution_center_id, warehouse_id FROM logistics_supply_chain_network order by city desc"
	go gopher_supply_chain_network(query, ch)
	fmt.Println(<-ch)

	// Query 3: print all zip-code entries in the logistics_supply_chain_network for city of Miami sorted by zipcode in descending order
	// For this query we are forking a goroutine to execute it

	query = fmt.Sprintf("SELECT zipcode, city, state, facility_id, distribution_center_id, warehouse_id FROM logistics_supply_chain_network where city='%s' order by zipcode", "Miami")
	go gopher_supply_chain_network(query, ch)
	fmt.Println(<-ch)

	// Query 4: print all zip-code details in the logistics_supply_chain_network for zipcode '60660'
	// For this query we are forking a goroutine to execute it

	query = fmt.Sprintf("SELECT zipcode, city, state, facility_id, distribution_center_id, warehouse_id FROM logistics_supply_chain_network where zipcode='%s'", "60660")
	go gopher_supply_chain_network(query, ch)
	fmt.Println(<-ch)

	// Requirement 1: print the total number of orders for every delivery Zipcode in transactions_log
	// Create a goroutine to execute this requirement

	// ADD_YOUR_CODE_BELOW
	requirement_1 := "SELECT deliveryzipcode, SUM(quantity) as count FROM transactions_log GROUP BY deliveryzipcode "
	go gopher_transactions_log(requirement_1, ch)
	fmt.Println(<-ch)

	// Requirement 2: print the total number of returned orders for every delivery Zipcode in transactions_log
	// Create a goroutine to execute this requirement

	// ADD_YOUR_CODE_BELOW
	requirement_2 := "SELECT deliveryzipcode, SUM(CASE WHEN orderreturned = 'Yes' THEN 1 ELSE 0 END) as count FROM transactions_log GROUP BY deliveryzipcode"
	go gopher_transactions_log(requirement_2, ch)
	fmt.Println(<-ch)

	// Requirement 3: get the list of warehouses with the count of orders returned for every warehouse in DESC order
	// Create a goroutine to execute this requirement

	// ADD_YOUR_CODE_BELOW
	requirement_3 := fmt.Sprintf("SELECT warehouse_id, SUM(CASE WHEN orderreturned = 'Yes' THEN 1 else 0 END) as count FROM transactions_log tl INNER JOIN logistics_supply_chain_network ln ON tl.deliveryzipcode = ln.zipcode GROUP BY warehouse_id ORDER by SUM(CASE WHEN orderreturned = 'Yes' THEN 1 ELSE 0 END) DESC")
	go gopher_warehouse_returns(requirement_3, ch)
	fmt.Println(<-ch)

	// Requirement 4: create 2 bar-charts for every zip.
	// First bar-chart to show total number of Transactions Ordered, Returned, Kept for every product category
	// Seconds bar-chart to show total total for Sales, Profit, Shippingcost for every product category
	// Three bars for every category in every chart
	// Create goroutines to execute this requirement
	// Also you must create a subdirectory called charts in your current workspace where all charts are saved

	number_of_goroutines := 0

	for _, v := range zip_codes_mp {
		// 2 goroutines for every zipcode to produce 2 charts

		// ADD_YOUR_CODE_BELOW
		number_of_goroutines = number_of_goroutines + 2

		// REMOVE the line below
		//number_of_goroutines = number_of_goroutines + 1

		go gopher_transactions_ordered_returned_kept(v.zipcode, ch)

		// ADD_YOUR_CODE_BELOW
		go gopher_total_sales_profit_shippingcost_chart(v.zipcode, ch)

		if number_of_goroutines == number_of_concurrent_gophers {
			for i := 0; i < number_of_goroutines; i++ {
				fmt.Println(<-ch)
			}
			number_of_goroutines = 0
		}

	}

	for i := 0; i < number_of_goroutines; i++ {
		fmt.Println(<-ch)

	}

	// Print how long it took the program to complete
	elapsed := time.Since(start)
	fmt.Println("program took", elapsed, "seconds")

}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

func fetch_zipcodes() {

	// Create the connection string
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"dbname=%s sslmode=disable",
		host, port, user, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	query := "SELECT zipcode, city, state, facility_id, distribution_center_id, warehouse_id FROM logistics_supply_chain_network"

	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&zipcode, &city, &state, &facility_id, &distribution_center_id, &warehouse_id)
		if err != nil {
			panic(err)
		}

		zip_codes_mp[zipcode] = WDC_ZipCode{
			zipcode, city, state, facility_id, distribution_center_id, warehouse_id,
		}

	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

func gopher_supply_chain_network(query string, ch chan<- string) {

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"dbname=%s sslmode=disable",
		host, port, user, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&zipcode, &city, &state, &facility_id, &distribution_center_id, &warehouse_id)
		if err != nil {
			panic(err)
		}
		fmt.Println("\n", zipcode, city, state, facility_id, distribution_center_id, warehouse_id)
	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

	ch <- "Completed: Goroutine for gopher_supply_chain_network"

}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

// ADD_YOUR_CODE_BELOW

func gopher_transactions_log(query string, ch chan<- string) {

	fmt.Println("Started: Goroutine gopher_transactions_log")

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"dbname=%s sslmode=disable",
		host, port, user, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&zipcode, &count)
		if err != nil {
			panic(err)
		}
		fmt.Println("\n", zipcode, count)
	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

	ch <- "Completed: Goroutine for gopher_transactions_log"

}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

func gopher_transactions_ordered_returned_kept(zipcode string, ch chan<- string) {

	fmt.Println("Started: Goroutine gopher_transactions_ordered_returned_kept for Zip Code = ", zipcode)

	var (
		category string
		count    string
	)

	var m = make(map[string]OrdersPerCategory)

	returned_orders := fmt.Sprintf("SELECT Category, count(Category) FROM transactions_log "+
		"WHERE  OrderReturned = 'Yes' AND deliveryzipcode='%s' "+
		"GROUP BY Category", zipcode)

	not_returned_orders := fmt.Sprintf("SELECT Category, count(Category) FROM transactions_log "+
		"WHERE  OrderReturned = 'No' AND deliveryzipcode='%s' "+
		"GROUP BY Category", zipcode)

	total_orders := fmt.Sprintf("SELECT Category, count(Category) FROM transactions_log "+
		"WHERE deliveryzipcode='%s' "+
		"GROUP BY Category", zipcode)

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"dbname=%s sslmode=disable",
		host, port, user, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Query Total Orders

	rows, err := db.Query(total_orders)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&category, &count)
		if err != nil {
			panic(err)
		}

		intCount, err := strconv.Atoi(count)
		if err != nil {
			panic(err)
		}

		m[category] = OrdersPerCategory{
			intCount, 0, 0,
		}

	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

	// Query returned Orders
	rows, err = db.Query(returned_orders)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&category, &count)
		if err != nil {
			panic(err)
		}

		intCount, err := strconv.Atoi(count)
		if err != nil {
			panic(err)
		}

		t := m[category]
		t.returned = intCount
		m[category] = t

	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

	rows, err = db.Query(not_returned_orders)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&category, &count)
		if err != nil {
			panic(err)
		}

		intCount, err := strconv.Atoi(count)
		if err != nil {
			panic(err)
		}

		t := m[category]
		t.not_returned = intCount
		m[category] = t

	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

	var totalOrdersGroup plotter.Values
	var returnedOrdersGroup plotter.Values
	var keptOrdersGroup plotter.Values

	category_labels := make([]string, len(m))

	i := 0
	for k, v := range m {
		totalOrdersGroup = append(totalOrdersGroup, float64(v.total))
		returnedOrdersGroup = append(returnedOrdersGroup, float64(v.returned))
		keptOrdersGroup = append(keptOrdersGroup, float64(v.not_returned))

		category_labels[i] = k
		i++

	}

	if len(totalOrdersGroup) == 0 {
		zipcode_goroutine_ch_msg := "No Data to Plot for Goroutine gopher_transactions_ordered_returned_kept for Zipcode = " + zipcode
		ch <- zipcode_goroutine_ch_msg
		return
	}

	bar_chart_label := fmt.Sprintf("Orders for Zip Code '%s' ", zipcode)

	p := plot.New()

	p.Title.Text = bar_chart_label
	p.Y.Label.Text = "Total"
	p.X.Tick.Label.Rotation = math.Pi / 4

	w := vg.Points(8)

	totalOrdersBar, err := plotter.NewBarChart(totalOrdersGroup, w)
	if err != nil {
		panic(err)
	}
	totalOrdersBar.LineStyle.Width = vg.Length(0)
	totalOrdersBar.Color = plotutil.Color(0)
	totalOrdersBar.Offset = -w

	returnedOrdersBar, err := plotter.NewBarChart(returnedOrdersGroup, w)
	if err != nil {
		panic(err)
	}
	returnedOrdersBar.LineStyle.Width = vg.Length(0)
	returnedOrdersBar.Color = plotutil.Color(1)

	keptOrdersBar, err := plotter.NewBarChart(keptOrdersGroup, w)
	if err != nil {
		panic(err)
	}
	keptOrdersBar.LineStyle.Width = vg.Length(0)
	keptOrdersBar.Color = plotutil.Color(2)
	keptOrdersBar.Offset = w

	p.Add(totalOrdersBar, returnedOrdersBar, keptOrdersBar)
	p.Legend.Add("All Orders", totalOrdersBar)
	p.Legend.Add("Returned Orders", returnedOrdersBar)
	p.Legend.Add("Kept Orders", keptOrdersBar)
	p.Legend.Top = true

	p.NominalX(category_labels...)

	barchart_file_name := "charts//" + "BarChart_Orders_ZipCode_" + zipcode + ".png"

	if err := p.Save(15*vg.Inch, 7.5*vg.Inch, barchart_file_name); err != nil {
		panic(err)
	}

	zipcode_goroutine_ch_msg := "Completed: Goroutine gopher_transactions_ordered_returned_kept for Zipcode = " + zipcode
	ch <- zipcode_goroutine_ch_msg

}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

// ADD_YOUR_CODE_BELOW

func gopher_total_sales_profit_shippingcost_chart(zipcode string, ch chan<- string) {

	fmt.Println("Started: Goroutine gopher_total_sales_profit_shippingcost_chart for Zip Code = ", zipcode)

	var (
		category string
		count    string
	)

	var m = make(map[string]SalesPerCategory)

	sales := fmt.Sprintf("SELECT Category, sum(sales) as count FROM transactions_log "+
		"WHERE  deliveryzipcode='%s' "+
		"GROUP BY Category", zipcode)

	shipping := fmt.Sprintf("SELECT Category, sum(shippingcost) as count FROM transactions_log "+
		"WHERE deliveryzipcode='%s' "+
		"GROUP BY Category", zipcode)

	profit := fmt.Sprintf("SELECT Category, sum(profit) as count FROM transactions_log "+
		"WHERE deliveryzipcode='%s' "+
		"GROUP BY Category", zipcode)

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"dbname=%s sslmode=disable",
		host, port, user, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Query Total Sales

	rows, err := db.Query(sales)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&category, &count)
		if err != nil {
			panic(err)
		}

		floatCount, err := strconv.ParseFloat(count, 64)
		if err != nil {
			panic(err)
		}

		m[category] = SalesPerCategory{
			floatCount, 0, 0,
		}

	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

	// Query returned Profit
	rows, err = db.Query(profit)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&category, &count)
		if err != nil {
			panic(err)
		}

		floatCount, err := strconv.ParseFloat(count, 64)
		if err != nil {
			panic(err)
		}

		t := m[category]
		t.profit = floatCount
		m[category] = t

	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

	rows, err = db.Query(shipping)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&category, &count)
		if err != nil {
			panic(err)
		}

		floatCount, err := strconv.ParseFloat(count, 64)
		if err != nil {
			panic(err)
		}

		t := m[category]
		t.shippingcost = floatCount
		m[category] = t

	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

	var totalSalesGroup plotter.Values
	var profitGroup plotter.Values
	var shippingGroup plotter.Values

	category_labels := make([]string, len(m))

	i := 0
	for k, v := range m {
		totalSalesGroup = append(totalSalesGroup, float64(v.sales))
		profitGroup = append(profitGroup, float64(v.profit))
		shippingGroup = append(shippingGroup, float64(v.shippingcost))

		category_labels[i] = k
		i++

	}

	if len(totalSalesGroup) == 0 {
		zipcode_goroutine_ch_msg := "No Data to Plot for Goroutine gopher_total_sales_profit_shippingcost_chart for Zipcode = " + zipcode
		ch <- zipcode_goroutine_ch_msg
		return
	}

	bar_chart_label := fmt.Sprintf("Sales for Zip Code '%s' ", zipcode)

	p := plot.New()

	p.Title.Text = bar_chart_label
	p.Y.Label.Text = "Total"
	p.X.Tick.Label.Rotation = math.Pi / 4

	w := vg.Points(8)

	totalSalesBar, err := plotter.NewBarChart(totalSalesGroup, w)
	if err != nil {
		panic(err)
	}
	totalSalesBar.LineStyle.Width = vg.Length(0)
	totalSalesBar.Color = plotutil.Color(0)
	totalSalesBar.Offset = -w

	profitBar, err := plotter.NewBarChart(profitGroup, w)
	if err != nil {
		panic(err)
	}
	profitBar.LineStyle.Width = vg.Length(0)
	profitBar.Color = plotutil.Color(1)

	shippingBar, err := plotter.NewBarChart(shippingGroup, w)
	if err != nil {
		panic(err)
	}
	shippingBar.LineStyle.Width = vg.Length(0)
	shippingBar.Color = plotutil.Color(2)
	shippingBar.Offset = w

	p.Add(totalSalesBar, profitBar, shippingBar)
	p.Legend.Add("Total Sales", totalSalesBar)
	p.Legend.Add("Profit", profitBar)
	p.Legend.Add("Shipping", shippingBar)
	p.Legend.Top = true

	p.NominalX(category_labels...)

	barchart_file_name := "charts//" + "BarChart_Sales_ZipCode_" + zipcode + ".png"

	if err := p.Save(15*vg.Inch, 7.5*vg.Inch, barchart_file_name); err != nil {
		panic(err)
	}

	zipcode_goroutine_ch_msg := "Completed: Goroutine gopher_transactions_ordered_returned_kept for Zipcode = " + zipcode
	ch <- zipcode_goroutine_ch_msg

}

///////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////

// ADD_YOUR_CODE_BELOW

func gopher_warehouse_returns(query string, ch chan<- string) {

	fmt.Println("Started: Goroutine gopher_warehouse_returns")

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"dbname=%s sslmode=disable",
		host, port, user, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&warehouse_id, &count)
		if err != nil {
			panic(err)
		}
		fmt.Println("\n", warehouse_id, count)
	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

	ch <- "Completed: Goroutine for gopher_warehouse_returns"

}
