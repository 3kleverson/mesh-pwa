"use strict";

const statusElement = document.querySelector("#status");
const nodesElement = document.querySelector("#nodes");
const refreshButton = document.querySelector("#refresh");

async function loadNodes() {
  statusElement.textContent = "Chargement de la configuration…";
  nodesElement.replaceChildren();

  try {
    const response = await fetch("./nodes.json", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);

    const config = await response.json();
    const nodes = Array.isArray(config.nodes) ? config.nodes : [];

    if (nodes.length === 0) {
      nodesElement.textContent = "Aucun nœud configuré.";
    } else {
      for (const node of nodes) {
        const item = document.createElement("li");
        const name = document.createElement("strong");
        name.textContent = node.name || "Nœud sans nom";
        item.append(name);

        if (node.signalUrl) {
          const detail = document.createElement("div");
          detail.className = "muted";
          detail.textContent = node.signalUrl;
          item.append(detail);
        }

        nodesElement.append(item);
      }
    }

    statusElement.textContent =
      `Configuration chargée. ${nodes.length} nœud(s) référencé(s). ` +
      "La connexion P2P n'est pas encore établie.";
  } catch (error) {
    statusElement.textContent =
      `Impossible de charger nodes.json : ${error.message}`;
    nodesElement.textContent = "Vérifie que nodes.json est bien publié.";
  }
}

refreshButton.addEventListener("click", loadNodes);

if ("serviceWorker" in navigator && location.protocol === "https:") {
  navigator.serviceWorker.register("./sw.js")
    .catch(error => console.error("Service worker :", error));
}

loadNodes();
