document.addEventListener("DOMContentLoaded", function () {
  const statusBox = document.getElementById("tsa-status");
  const lastUpdatedBox = document.getElementById("tsa-last-updated");

  fetch("/wp-content/uploads/tsa-data/teams/wordpress_team_refresh_meta.json")
    .then(response => response.json())
    .then(meta => {
      const raw = meta.finished_at.replace(" ", "T");
      const date = new Date(raw);

      const formatted = date.toLocaleString("en-CA", {
        timeZone: "America/Vancouver",
        year: "numeric",
        month: "long",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
        hour12: true,
        timeZoneName: "short"
      });

      lastUpdatedBox.textContent = "Team data updated: " + formatted;
    })
    .catch(() => {
      lastUpdatedBox.textContent = "Team data updated: Unavailable";
    });

  function setStatus(message, type = "info") {
    statusBox.textContent = message;
    statusBox.className = "tsa-status " + type;
  }

  function setDownloadButtonsDisabled(disabled) {
    document.getElementById("tsa-download-filtered").disabled = disabled;
    document.getElementById("tsa-download-full").disabled = disabled;
  }

  let currentEndpoint = "/wp-json/tsa/v1/team-summary";
  let currentDataset = "summary";

  let defaultStartDate = "";
  let defaultEndDate = "";
  let tsaFilterTimer;

  const teamSelect = new TomSelect("#tsa-team-filter", {
    plugins: ["remove_button"],
    create: false,
    maxItems: null,
    placeholder: "All Teams",
    closeAfterSelect: false
  });

  const opponentSelect = new TomSelect("#tsa-opponent-filter", {
    plugins: ["remove_button"],
    create: false,
    maxItems: null,
    placeholder: "All Opponents",
    closeAfterSelect: false
  });

  function populateTomSelect(selectInstance, values) {
    selectInstance.clear();
    selectInstance.clearOptions();

    values.forEach(value => {
      selectInstance.addOption({
        value: value,
        text: value
      });
    });

    selectInstance.refreshOptions(false);
  }

  fetch("/wp-json/tsa/v1/team-team-options")
    .then(response => response.json())
    .then(data => {
      populateTomSelect(teamSelect, data.teams || []);
      populateTomSelect(opponentSelect, data.opponents || []);
    })
    .catch(() => {
      console.warn("Unable to load dynamic team lists.");
    });

  function getMetaEndpoint() {
    return `/wp-json/tsa/v1/team-${currentDataset}-meta`;
  }

  function getCsvEndpoint(full = false) {
    const base = `/wp-json/tsa/v1/team-${currentDataset}-csv`;
    return full ? `${base}?full=1` : base;
  }

  const table = new Tabulator("#tsa-table", {
    ajaxURL: currentEndpoint,
    ajaxConfig: "GET",
    ajaxParams: function () {
      const mode = document.querySelector('input[name="tsa-date-mode"]:checked').value;

      return {
        search: document.getElementById("tsa-search").value,
        teams: teamSelect.getValue().join(","),
        opponents: opponentSelect.getValue().join(","),
        homeRoad: document.getElementById("tsa-homeroad-filter").value,
        date_single: mode === "single" ? document.getElementById("tsa-date-single").value : "",
        date_start: mode === "range" ? document.getElementById("tsa-date-start").value : "",
        date_end: mode === "range" ? document.getElementById("tsa-date-end").value : ""
      };
    },
    sortMode: "remote",
    pagination: true,
    paginationMode: "remote",
    paginationSize: 10,
    paginationSizeSelector: [10, 25, 50, 100],
    layout: "fitDataStretch",
    autoColumns: true,

    ajaxResponse: function (url, params, response) {
      setDownloadButtonsDisabled(false);

      const total = Number(response.total || 0);

      if (total === 0) {
        setStatus("No rows match your current filters.", "empty");
      } else {
        setStatus("Matching rows: " + total.toLocaleString(), "success");
      }

      return response;
    },

    ajaxError: function () {
      setDownloadButtonsDisabled(false);
      setStatus("Dataset failed to load. Please refresh the page or try another dataset.", "error");
    }
  });

  function refreshTable() {
    clearTimeout(tsaFilterTimer);

    setStatus("Loading dataset...", "loading");
    setDownloadButtonsDisabled(true);

    tsaFilterTimer = setTimeout(function () {
      table.setPage(1);
      table.setData(currentEndpoint);
    }, 300);
  }

  function updateDateMode() {
    const mode = document.querySelector('input[name="tsa-date-mode"]:checked').value;

    document.getElementById("tsa-single-wrap").style.display =
      mode === "single" ? "block" : "none";

    document.getElementById("tsa-range-start-wrap").style.display =
      mode === "range" ? "block" : "none";

    document.getElementById("tsa-range-end-wrap").style.display =
      mode === "range" ? "block" : "none";

    refreshTable();
  }

  function updateDatasetUI() {
    fetch(getMetaEndpoint())
      .then(response => response.json())
      .then(meta => {
        defaultStartDate = meta.min_date;
        defaultEndDate = meta.max_date;

        document.getElementById("tsa-date-start").value = defaultStartDate;
        document.getElementById("tsa-date-end").value = defaultEndDate;
        document.getElementById("tsa-date-single").value = defaultEndDate;

        updateDateMode();
        table.setData(currentEndpoint);
      });
  }

  document.querySelectorAll(".tsa-dataset-btn").forEach(function (btn) {
    btn.addEventListener("click", function () {
      document.querySelectorAll(".tsa-dataset-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");

      currentEndpoint = btn.dataset.endpoint;
      currentDataset = btn.dataset.dataset;

      updateDatasetUI();
    });
  });

  document.getElementById("tsa-sidebar-toggle").addEventListener("click", function () {
    const sidebar = document.getElementById("tsa-sidebar");
    const button = document.getElementById("tsa-sidebar-toggle");

    sidebar.classList.toggle("collapsed");

    const expanded = !sidebar.classList.contains("collapsed");
    button.setAttribute("aria-expanded", expanded);
  });

  document.querySelectorAll('input[name="tsa-date-mode"]').forEach(function (el) {
    el.addEventListener("change", updateDateMode);
  });

  document.getElementById("tsa-search").addEventListener("input", refreshTable);
  document.getElementById("tsa-date-single").addEventListener("change", refreshTable);
  document.getElementById("tsa-date-start").addEventListener("change", refreshTable);
  document.getElementById("tsa-date-end").addEventListener("change", refreshTable);
  document.getElementById("tsa-homeroad-filter").addEventListener("change", refreshTable);
  teamSelect.on("change", refreshTable);
  opponentSelect.on("change", refreshTable);

  document.getElementById("tsa-reset").addEventListener("click", function () {
    document.getElementById("tsa-search").value = "";
    document.getElementById("tsa-homeroad-filter").value = "";
    document.querySelector('input[name="tsa-date-mode"][value="single"]').checked = true;

    teamSelect.clear();
    opponentSelect.clear();

    document.getElementById("tsa-date-start").value = defaultStartDate;
    document.getElementById("tsa-date-end").value = defaultEndDate;
    document.getElementById("tsa-date-single").value = defaultEndDate;

    updateDateMode();
  });

  function getCurrentParams() {
    const mode = document.querySelector('input[name="tsa-date-mode"]:checked').value;

    return {
      search: document.getElementById("tsa-search").value,
      teams: teamSelect.getValue().join(","),
      opponents: opponentSelect.getValue().join(","),
      homeRoad: document.getElementById("tsa-homeroad-filter").value,
      date_single: mode === "single" ? document.getElementById("tsa-date-single").value : "",
      date_start: mode === "range" ? document.getElementById("tsa-date-start").value : "",
      date_end: mode === "range" ? document.getElementById("tsa-date-end").value : ""
    };
  }

  document.getElementById("tsa-download-filtered").addEventListener("click", function () {
    if (table.getDataCount("active") === 0) {
      alert("No rows to download for the current filters.");
      return;
    }

    const params = new URLSearchParams(getCurrentParams());
    window.open(getCsvEndpoint(false) + "?" + params.toString(), "_blank");
  });

  document.getElementById("tsa-download-full").addEventListener("click", function () {
    window.open(getCsvEndpoint(true), "_blank");
  });

  updateDatasetUI();
});