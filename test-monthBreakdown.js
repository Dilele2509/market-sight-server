// Simple test for monthly breakdown logic
function getLastDayOfMonth(year, month) {
  const lastDay = new Date(year, month + 1, 0);
  return lastDay.toISOString().split('T')[0];
}

function getFirstDayOfMonth(year, month) {
  const firstDay = new Date(year, month, 1);
  return firstDay.toISOString().split('T')[0];
}

function testMonthlyBreakdown(start_date, end_date) {
  console.log(`Testing breakdown from ${start_date} to ${end_date}`);
  
  // Parse dates
  const startDate = new Date(start_date);
  const endDate = new Date(end_date);
  
  // Get the year and month of the start and end dates
  const startYear = startDate.getFullYear();
  const startMonth = startDate.getMonth();
  const endYear = endDate.getFullYear();
  const endMonth = endDate.getMonth();
  
  // Create array to hold periods
  const months = [];
  
  // Iterate through each month in the range
  let currentYear = startYear;
  let currentMonth = startMonth;
  
  while (
    currentYear < endYear || 
    (currentYear === endYear && currentMonth <= endMonth)
  ) {
    let periodStart, periodEnd;
    
    // For the first month, use the given start date
    if (currentYear === startYear && currentMonth === startMonth) {
      periodStart = startDate.toISOString().split('T')[0];
    } else {
      // For other months, start on the 1st
      periodStart = getFirstDayOfMonth(currentYear, currentMonth);
    }
    
    // For the last month, use the given end date
    if (currentYear === endYear && currentMonth === endMonth) {
      periodEnd = endDate.toISOString().split('T')[0];
    } else {
      // For other months, end on the last day
      periodEnd = getLastDayOfMonth(currentYear, currentMonth);
    }
    
    // Add the period to the list
    months.push({
      period_start: periodStart,
      period_end: periodEnd
    });
    
    // Log this calculation
    const logLabel = 
      (currentYear === startYear && currentMonth === startMonth) ? 'First month:' :
      (currentYear === endYear && currentMonth === endMonth) ? 'Last month:' :
      'Middle month:';
    
    console.log(`${logLabel} ${periodStart} to ${periodEnd} (Month: ${currentMonth + 1}, Year: ${currentYear})`);
    
    // Move to the next month
    currentMonth++;
    if (currentMonth > 11) {
      currentMonth = 0;
      currentYear++;
    }
  }
  
  console.log("Final periods:", months);
  console.log("------------------------");
}

// Test cases
testMonthlyBreakdown("2025-03-01", "2025-05-06");  // Current example
testMonthlyBreakdown("2025-03-15", "2025-05-20");  // Mid-month to mid-month
testMonthlyBreakdown("2025-01-15", "2025-03-10");  // Spanning February (28/29 days)
testMonthlyBreakdown("2025-02-01", "2025-02-28");  // Single month (February)
testMonthlyBreakdown("2025-11-15", "2026-01-15");  // Spanning year boundary 