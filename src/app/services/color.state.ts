import { effect, inject, Service, signal } from '@angular/core';
import { applyEach, disabled, form, max, min } from '@angular/forms/signals';
import { UtilsService } from './utils.service';

export interface ColorModel {
    name: string,
    hue: number,
    hueDiff: number,
    lockHue: boolean,
    saturation: number,
    lockSaturation: boolean,
    shades: number[],
    lockShade: boolean
}

export interface PaletteModel {
    name: string;
    colors: ColorModel[];
}

export type ColorStateModel = PaletteModel

@Service()
export class ColorState {
    utilsService = inject(UtilsService)

    inc = signal<number>(0)

    state = signal<ColorStateModel>({
        name: '',
        colors: []
    })

    stateForm = form(
        this.state,
        (path) => {
            applyEach(path.colors, (colorPath) => {
                applyEach(colorPath.shades, (shadePath) => {
                    min(shadePath, 0);
                    max(shadePath, 1);
                    disabled(shadePath, {
                        when: ({ valueOf }) => valueOf(colorPath.lockShade),
                    })
                });
            });
        }
    )

    _ = effect(() => {
        this._updateColors(this.state())
        localStorage.setItem('currentColor', JSON.stringify(this.state())) 
    })

    constructor() {
        const current = localStorage.getItem('currentColor') || ''
        //new palette
        if(!current) {
            this.reset()
        }
        //old
        else {
            const parsed = JSON.parse(current)
            
            this.inc.set(0)
            this.setColor(
                parsed.name || "unknown", 
                (parsed.colors || []).map((el:any, i: number)=>this._createColor(i === 0, el))
            )
        }
    }

    setColor = (name: string, colors: ColorModel[]) => {
        this.state.set({
            name,
            colors: [
                ...colors
            ]
        })
    }

    reset = () => {
        this.inc.set(0)
        this.setColor("New Palette", [
            this._createColor(true)
        ])
    }

    addShade = (colorIndex: number) => {
        const current =  this.state().colors.find((_, i)=>i === colorIndex);

        if(!current) {
            return;
        }

        this.state.set({
            ...this.state(),
            colors: [
                ...this.state().colors.map((el, i)=>{
                    if (i != colorIndex) {
                        return el
                    }
                    return {
                        ...current,
                        shades: [
                                ...current?.shades,
                                0.5
                        ]
                    }
                }),
            ]
        })
    }

    addColor = () => {
        this.state.set({
            ...this.state(),
            colors: [
                ...this.state().colors,
                this._createColor(false)
            ]
        })
    }

    removeShade = (colorIndex: number, shadeIndex: number) => {
        const current =  this.state().colors.find((_, i)=>i === colorIndex);

        if(!current) {
            return;
        }

        this.state.set({
            ...this.state(),
            colors: [
                ...this.state().colors.map((el, i)=>{
                    if (i != colorIndex) {
                        return el
                    }
                    return {
                        ...current,
                        shades: [
                                ...current?.shades.filter((_, i)=>i != shadeIndex),
                        ]
                    }
                }),
            ]
        })
    }
                
    removeColor = (colorIndex: number) => {
        const current =  this.state().colors.find((_, i)=>i === colorIndex);

        if(!current) {
            return;
        }

        this.state.set({
            ...this.state(),
            colors: [
                ...this.state().colors.filter((_, i)=>i != colorIndex)
            ]
        })
    }

    private _updateColors(params: ColorStateModel) {
        const colors = params.colors

        const mainColor = colors[0];

        if (colors.length > 1) {        
            for (let i = 1; i < colors.length; i++) {
                const currentColor = colors[i];
                if(currentColor.lockHue) {
                    currentColor.hue = this.utilsService.addHue(mainColor.hue, currentColor.hueDiff)
                }
                else {
                    currentColor.hueDiff = currentColor.hue - mainColor.hue
                }

                if(currentColor.lockSaturation) {
                    currentColor.saturation = mainColor.saturation
                }

                if(currentColor.lockShade) {
                    currentColor.shades = [...mainColor.shades]
                }
            }
        }
    }

    private _createColor(first: boolean = false, data: any = {}):ColorModel {
        const increment = () => {
            const cur = this.inc()
            this.inc.set(cur+1);
            return cur;
        }

        const inc = increment()

        return {
            name: data.name || `Color ${inc}`,
            hue: data.hue || 15, 
            hueDiff: data.hueDiff || 0,
            lockHue: data.lockHue || !first,
            saturation: data.saturation || 0.52,
            lockSaturation: data.lockSaturation || !first,
            shades: data.shades || [
                0.23, 0.43, 0.63, 0.83
            ],
            lockShade: data.lockShade || !first
        }
    }
}
