#!/usr/bin/env bash
set -euo pipefail

# commit.sh - helper para este repo
# Uso:
#   ./commit.sh -m "mensaje"         # add ., commit y push
#   ./commit.sh -m "msg" -n          # add ., commit (no push)
#   ./commit.sh -m "msg" -f file1 file2  # add archivos específicos
#   ./commit.sh -s                    # omitir pre-checks (black/flake8)

show_help() {
	sed -n '1,120p' "$0" | sed -n '1,8p'
	echo
	echo "Opciones:" 
	echo "  -m MSG     Mensaje de commit (por defecto: 'Actualización')"
	echo "  -n         No hacer push (solo commit local)"
	echo "  -s         Saltar pre-checks (black / flake8)"
	echo "  -f FILES   Archivos a añadir (por defecto: todo con 'git add .')"
	echo "  -h         Mostrar ayuda"
}

MSG="Actualización"
NO_PUSH=0
SKIP_CHECKS=0
FILES=()

while [[ ${#-} -ge 0 ]] && [[ $# -gt 0 ]]; do
	case "$1" in
		-m)
			shift
			MSG="$1"
			shift
			;;
		-n)
			NO_PUSH=1
			shift
			;;
		-s)
			SKIP_CHECKS=1
			shift
			;;
		-f)
			shift
			# collect remaining args until a flag or end
			while [[ $# -gt 0 && ! "$1" =~ ^- ]]; do
				FILES+=("$1")
				shift
			done
			;;
		-h|--help)
			show_help
			exit 0
			;;
		*)
			echo "Opción desconocida: $1"
			show_help
			exit 1
			;;
	esac
done

# Mostrar entorno actual (útil cuando se trabaja con conda)
if [[ -n "${CONDA_DEFAULT_ENV-}" ]]; then
	echo "Conda env activo: ${CONDA_DEFAULT_ENV}"
else
	echo "(No se detectó entorno conda activo)"
fi

REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || true)
if [[ -n "$REPO_ROOT" ]]; then
	cd "$REPO_ROOT"
fi

echo "Commit message: $MSG"

if [[ $SKIP_CHECKS -eq 0 ]]; then
	# Formatear con black si está disponible
	if command -v black >/dev/null 2>&1; then
		echo "Running black..."
		black --quiet . || true
	fi
	# Ejecutar flake8 si está disponible
	if command -v flake8 >/dev/null 2>&1; then
		echo "Running flake8..."
		flake8 || echo "flake8 returned non-zero (warnings/errors)"
	fi
fi

if [[ ${#FILES[@]} -gt 0 ]]; then
	echo "git add ${FILES[*]}"
	git add "${FILES[@]}"
else
	echo "git add ."
	git add .
fi

git commit -m "$MSG" || {
	echo "No hay cambios para commitear."
	exit 0
}

BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ $NO_PUSH -eq 0 ]]; then
	echo "Pushing to origin/$BRANCH..."
	git push origin "$BRANCH"
else
	echo "Commit creado localmente en rama $BRANCH (no push)."
fi

echo "Hecho."
