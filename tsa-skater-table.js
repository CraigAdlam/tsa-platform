window.addEventListener("load", async function () {
  const debugEl = document.getElementById("tsa-debug");
  if (!debugEl) return;

  const log = (msg) => {
    console.log("[TSA]", msg);
    debugEl.innerHTML += "<br>" + msg;
  };

  try {
    log("Window loaded.");

    const ajaxURL = "https://topshelfanalytics.com/wp-content/uploads/data/skater_summary.json";
    const teamFilter = document.getElementById("tsa-team-filter");
    const searchInput = document.getElementById("tsa-search");
    const pageSizeSelect = document.getElementById("tsa-page-size");
    const singleDateInput = document.getElementById("tsa-date-single");
    const startDateInput = document.getElementById("tsa-date-start");
    const endDateInput = document.getElementById("tsa-date-end");
    const clearDatesBtn = document.getElementById("tsa-clear-dates");

    if (!window.Tabulator) {
      throw new Error("Tabulator library not available.");
    }
    log("Tabulator library found.");

    const response = await fetch(ajaxURL, { cache: "no-store" });
    log("Fetch complete. Status: " + response.status);

    if (!response.ok) {
      throw new Error("Fetch failed with status " + response.status);
    }

    const rawData = await response.json();
    log("JSON parsed.");

    if (!Array.isArray(rawData)) {
      throw new Error("JSON is not an array.");
    }

    log("Rows loaded: " + rawData.length);

    const cleanedData = rawData.map(row => {
      const out = { ...row };
      if (out.gameDate) out.gameDate = String(out.gameDate).slice(0, 10);
      return out;
    });

    const teams = [...new Set(cleanedData.map(r => r.teamAbbrev).filter(Boolean))].sort();
    log("Teams found: " + teams.length);

    teamFilter.innerHTML =
      '<option value="">All teams</option>' +
      teams.map(t => `<option value="${t}">${t}</option>`).join("");

    log("Team dropdown populated.");

	const columns = [
	  { title: "Date", field: "gameDate", sorter: "date", sorterParams: {format: "yyyy-MM-dd", alignEmptyValues: "bottom"}, width: 115, headerFilter: "input" },
	  { title: "Game ID", field: "gameId", sorter: "number", width: 115, headerFilter: "input" },
	  { title: "Player", field: "skaterFullName", sorter: "string", minWidth: 190, headerFilter: "input", frozen: true },
	  { title: "Player ID", field: "playerId", sorter: "number", width: 105, headerFilter: "input" },
	  { title: "H/R", field: "homeRoad", sorter: "string", width: 80, headerFilter: "input" },
	  { title: "Team", field: "teamAbbrev", sorter: "string", width: 90, headerFilter: "input" },
	  { title: "Opp", field: "opponentTeamAbbrev", sorter: "string", width: 90, headerFilter: "input" },
	  { title: "Pos", field: "positionCode", sorter: "string", width: 80, headerFilter: "input" },
	  { title: "GP", field: "gamesPlayed", sorter: "number", width: 80, hozAlign: "center", headerFilter: "input" },
	  { title: "G", field: "goals", sorter: "number", width: 75, hozAlign: "center", headerFilter: "input" },
	  { title: "A", field: "assists", sorter: "number", width: 75, hozAlign: "center", headerFilter: "input" },
	  { title: "PTS", field: "points", sorter: "number", width: 80, hozAlign: "center", headerFilter: "input" },
	  { title: "Shots", field: "shots", sorter: "number", width: 90, hozAlign: "center", headerFilter: "input" },
	  { title: "Shooting %", field: "shootingPct", sorter: "number", width: 110, hozAlign: "center", headerFilter: "input" },
	  { title: "TOI / Game", field: "timeOnIcePerGame", sorter: "number", width: 120, hozAlign: "center", headerFilter: "input" }
	];

    const table = new Tabulator("#tsa-table", {
      data: cleanedData,
      layout: "fitDataStretch",
      height: "700px",
      pagination: true,
      paginationMode: "local",
      paginationSize: 25,
      paginationSizeSelector: [10, 25, 50, 100],
      movableColumns: true,
      placeholder: "No data found",
      index: "playerId",
	  initialSort: [
		{ column: "gameDate", dir: "desc" },
		{ column: "skaterFullName", dir: "asc" }
	  ],
      columns: columns
    });

    log("Tabulator table created.");

    function applyFilters() {
      const searchValue = searchInput.value.trim().toLowerCase();
      const teamValue = teamFilter.value;
      const singleDate = singleDateInput.value;
      const startDate = startDateInput.value;
      const endDate = endDateInput.value;

      table.setFilter(function(data) {
        const rowDate = data.gameDate ? String(data.gameDate).slice(0, 10) : "";

        if (teamValue && data.teamAbbrev !== teamValue) return false;

        if (searchValue) {
          const matchesSearch =
            String(data.skaterFullName || "").toLowerCase().includes(searchValue) ||
            String(data.teamAbbrev || "").toLowerCase().includes(searchValue) ||
            String(data.opponentTeamAbbrev || "").toLowerCase().includes(searchValue) ||
            String(data.positionCode || "").toLowerCase().includes(searchValue) ||
            String(data.gameDate || "").toLowerCase().includes(searchValue);

          if (!matchesSearch) return false;
        }

        if (singleDate) return rowDate === singleDate;
        if (startDate && rowDate < startDate) return false;
        if (endDate && rowDate > endDate) return false;

        return true;
      });
    }

    searchInput.addEventListener("input", applyFilters);
    teamFilter.addEventListener("change", applyFilters);

    pageSizeSelect.addEventListener("change", function () {
      table.setPageSize(Number(this.value));
    });

    singleDateInput.addEventListener("change", function () {
      if (this.value) {
        startDateInput.value = "";
        endDateInput.value = "";
      }
      applyFilters();
    });

    startDateInput.addEventListener("change", function () {
      if (this.value) singleDateInput.value = "";
      applyFilters();
    });

    endDateInput.addEventListener("change", function () {
      if (this.value) singleDateInput.value = "";
      applyFilters();
    });

    clearDatesBtn.addEventListener("click", function () {
      singleDateInput.value = "";
      startDateInput.value = "";
      endDateInput.value = "";
      applyFilters();
    });

    log("Filters wired up.");
    log("Done.");
  } catch (err) {
    console.error("[TSA ERROR]", err);
    debugEl.innerHTML += "<br><strong style='color:#b00020;'>ERROR: " + err.message + "</strong>";
  }
});